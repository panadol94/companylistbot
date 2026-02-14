"""
Media Grid Collage Generator
Combines multiple media (photos/videos) into a single grid collage with watermark.
Videos play in the grid using FFmpeg subprocess — fast, low memory.
"""
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import logging
import tempfile
import subprocess
import os
import re

logger = logging.getLogger(__name__)

# Grid config
GRID_GAP = 6
GRID_BG_HEX = '0x141414'  # for FFmpeg
GRID_BG = (20, 20, 20)     # for PIL
CORNER_RADIUS = 16
OUTPUT_WIDTH = 1280
OUTPUT_QUALITY = 92
WATERMARK_OPACITY = 180
GRADIENT_HEIGHT = 60


def create_grid_collage(media_list: list, watermark_text: str = ""):
    """
    Create a grid collage from multiple media items.
    Returns (bytes, is_video) tuple or None.
    """
    if not media_list or len(media_list) < 2:
        return None
    
    has_video = any(mt == 'video' for _, mt in media_list)
    
    if has_video:
        return _create_mosaic_video_ffmpeg(media_list, watermark_text)
    else:
        return _create_static_grid(media_list, watermark_text)


def _create_static_grid(media_list, watermark_text):
    """Create static JPEG grid for photos only."""
    images = []
    for media_bytes, media_type in media_list:
        try:
            img = Image.open(BytesIO(media_bytes)).convert('RGB')
            images.append(img)
        except Exception as e:
            logger.error(f"Grid: failed to process photo: {e}")
            images.append(_create_placeholder("⚠️", (600, 400)))
    
    if len(images) < 2:
        return None
    
    grid = _build_grid(images)
    grid = _add_gradient(grid)
    if watermark_text:
        grid = _add_watermark(grid, watermark_text)
    
    output = BytesIO()
    grid.save(output, format='JPEG', quality=OUTPUT_QUALITY, optimize=True)
    output.seek(0)
    return (output.getvalue(), False)


def _create_mosaic_video_ffmpeg(media_list, watermark_text):
    """
    Create mosaic video using FFmpeg subprocess.
    FFmpeg handles everything natively in C — fast & low memory.
    """
    n = min(len(media_list), 6)
    gap = GRID_GAP
    
    # Cell dimensions (must be even for h264)
    cell_w = (OUTPUT_WIDTH - gap) // 2
    cell_w = cell_w - (cell_w % 2)  # ensure even
    cell_h = int(cell_w * 0.75)
    cell_h = cell_h - (cell_h % 2)  # ensure even
    
    # Canvas size
    if n == 2:
        rows = 1
    elif n == 3:
        rows = 2
    else:
        rows = (n + 1) // 2
    
    canvas_w = cell_w * 2 + gap
    canvas_h = cell_h * rows + gap * (rows - 1)
    # Ensure even
    canvas_w = canvas_w + (canvas_w % 2)
    canvas_h = canvas_h + (canvas_h % 2)
    
    temp_files = []
    
    try:
        # Save media to temp files & detect max video duration
        inputs = []
        max_duration = 0
        
        for idx, (media_bytes, media_type) in enumerate(media_list[:6]):
            suffix = '.mp4' if media_type == 'video' else '.jpg'
            tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
            tmp.write(media_bytes)
            tmp.close()
            temp_files.append(tmp.name)
            inputs.append({'path': tmp.name, 'type': media_type})
            
            # Get video duration
            if media_type == 'video':
                dur = _get_video_duration(tmp.name)
                if dur > max_duration:
                    max_duration = dur
        
        if max_duration <= 0:
            max_duration = 10
        
        # Calculate cell positions
        positions = _get_cell_positions(n, cell_w, cell_h, gap)
        
        # Build FFmpeg command
        cmd = ['ffmpeg', '-y']
        
        # Input: background color
        cmd.extend([
            '-f', 'lavfi', '-i',
            f'color=c={GRID_BG_HEX}:s={canvas_w}x{canvas_h}:d={max_duration}:r=30'
        ])
        
        # Add each media as input
        for inp in inputs:
            if inp['type'] != 'video':
                cmd.extend(['-loop', '1', '-t', str(max_duration)])
            cmd.extend(['-i', inp['path']])
        
        # Build filter_complex
        filters = []
        
        # Scale each input to cell size (input 0 is background, so media starts from 1)
        for i in range(n):
            input_idx = i + 1  # offset by 1 because of background
            filters.append(
                f'[{input_idx}:v]scale={cell_w}:{cell_h}:'
                f'force_original_aspect_ratio=decrease,'
                f'pad={cell_w}:{cell_h}:(ow-iw)/2:(oh-ih)/2:'
                f'color={GRID_BG_HEX},setsar=1[v{i}]'
            )
        
        # Overlay each cell onto background
        prev = '0:v'
        for i in range(n):
            x, y = positions[i]
            out_label = f'tmp{i}'
            filters.append(
                f'[{prev}][v{i}]overlay={x}:{y}:shortest=1[{out_label}]'
            )
            prev = out_label
        
        # Add watermark with drawtext
        if watermark_text:
            safe_text = watermark_text.replace("'", "\\'").replace(":", "\\:")
            filters.append(
                f'[{prev}]drawtext=text=\'{safe_text}\':'
                f'x=w-tw-28:y=h-th-20:'
                f'fontsize=22:fontcolor=white:'
                f'box=1:boxcolor=black@0.7:boxborderw=8[out]'
            )
            final_label = '[out]'
        else:
            final_label = f'[{prev}]'
        
        filter_str = ';'.join(filters)
        
        # Output file
        out_tmp = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False)
        out_tmp.close()
        temp_files.append(out_tmp.name)
        
        cmd.extend([
            '-filter_complex', filter_str,
            '-map', final_label,
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-crf', '23',
            '-pix_fmt', 'yuv420p',
            '-t', str(max_duration),
            '-movflags', '+faststart',
            out_tmp.name
        ])
        
        logger.info(f"FFmpeg grid: {n} inputs, {max_duration:.1f}s, {canvas_w}x{canvas_h}")
        
        # Run FFmpeg
        timeout = max(max_duration * 3, 120)
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        
        if result.returncode != 0:
            logger.error(f"FFmpeg failed (rc={result.returncode}): {result.stderr[-500:]}")
            return None
        
        # Read output
        if not os.path.exists(out_tmp.name):
            return None
        
        with open(out_tmp.name, 'rb') as f:
            result_bytes = f.read()
        
        if not result_bytes or len(result_bytes) < 1000:
            logger.error("FFmpeg output too small or empty")
            return None
        
        logger.info(f"FFmpeg grid done: {len(result_bytes)} bytes")
        return (result_bytes, True)
    
    except subprocess.TimeoutExpired:
        logger.error("FFmpeg timed out")
        return None
    except Exception as e:
        logger.error(f"FFmpeg mosaic failed: {e}")
        return None
    finally:
        for tf in temp_files:
            try:
                os.unlink(tf)
            except Exception:
                pass


def _get_video_duration(filepath):
    """Get video duration in seconds using ffprobe/ffmpeg."""
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', filepath],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except Exception:
        pass
    
    # Fallback: parse ffmpeg output
    try:
        result = subprocess.run(
            ['ffmpeg', '-i', filepath], capture_output=True, text=True, timeout=10
        )
        match = re.search(r'Duration: (\d+):(\d+):(\d+\.\d+)', result.stderr)
        if match:
            h, m, s = match.groups()
            return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception:
        pass
    
    return 0


def _get_cell_positions(n, cell_w, cell_h, gap):
    """Get (x, y) position for each cell in the grid."""
    if n == 2:
        return [(0, 0), (cell_w + gap, 0)]
    elif n == 3:
        canvas_w = cell_w * 2 + gap
        return [(0, 0), (cell_w + gap, 0),
                ((canvas_w - cell_w) // 2, cell_h + gap)]
    else:
        positions = []
        for i in range(min(n, 6)):
            row, col = i // 2, i % 2
            positions.append((col * (cell_w + gap), row * (cell_h + gap)))
        return positions


# --- Static grid helpers (for photos-only) ---

def _create_placeholder(text, size):
    img = Image.new('RGB', size, (60, 60, 70))
    draw = ImageDraw.Draw(img)
    font = _get_font(32)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(((size[0]-tw)//2, (size[1]-th)//2), text, fill=(200,200,200), font=font)
    return img


def _resize_to_fill(img, target_w, target_h):
    w, h = img.size
    if w == 0 or h == 0:
        return Image.new('RGB', (target_w, target_h), GRID_BG)
    scale = min(target_w / w, target_h / h)
    new_w, new_h = int(w * scale), int(h * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    canvas = Image.new('RGB', (target_w, target_h), GRID_BG)
    canvas.paste(img, ((target_w-new_w)//2, (target_h-new_h)//2))
    return canvas


def _round_corners(img, radius):
    if radius <= 0:
        return img
    mask = Image.new('L', img.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle([(0, 0), img.size], radius=radius, fill=255)
    bg = Image.new('RGB', img.size, GRID_BG)
    bg.paste(img, mask=mask)
    return bg


def _build_grid(images):
    n = len(images)
    gap = GRID_GAP
    cell_w = (OUTPUT_WIDTH - gap) // 2
    cell_h = int(cell_w * 0.75)

    if n == 2:
        img1 = _round_corners(_resize_to_fill(images[0], cell_w, cell_h), CORNER_RADIUS)
        img2 = _round_corners(_resize_to_fill(images[1], cell_w, cell_h), CORNER_RADIUS)
        canvas = Image.new('RGB', (OUTPUT_WIDTH, cell_h), GRID_BG)
        canvas.paste(img1, (0, 0))
        canvas.paste(img2, (cell_w + gap, 0))
        return canvas
    elif n == 3:
        img1 = _round_corners(_resize_to_fill(images[0], cell_w, cell_h), CORNER_RADIUS)
        img2 = _round_corners(_resize_to_fill(images[1], cell_w, cell_h), CORNER_RADIUS)
        img3 = _round_corners(_resize_to_fill(images[2], cell_w, cell_h), CORNER_RADIUS)
        total_h = cell_h * 2 + gap
        canvas = Image.new('RGB', (OUTPUT_WIDTH, total_h), GRID_BG)
        canvas.paste(img1, (0, 0))
        canvas.paste(img2, (cell_w + gap, 0))
        canvas.paste(img3, ((OUTPUT_WIDTH - cell_w) // 2, cell_h + gap))
        return canvas
    else:
        rows = (min(n, 6) + 1) // 2
        total_h = cell_h * rows + gap * (rows - 1)
        canvas = Image.new('RGB', (OUTPUT_WIDTH, total_h), GRID_BG)
        for i, img in enumerate(images[:6]):
            row, col = i // 2, i % 2
            cell = _round_corners(_resize_to_fill(img, cell_w, cell_h), CORNER_RADIUS)
            canvas.paste(cell, (col * (cell_w + gap), row * (cell_h + gap)))
        return canvas


def _add_gradient(img):
    img = img.copy()
    w, h = img.size
    gradient = Image.new('RGBA', (w, GRADIENT_HEIGHT), (0, 0, 0, 0))
    draw = ImageDraw.Draw(gradient)
    for y in range(GRADIENT_HEIGHT):
        alpha = int(160 * (y / GRADIENT_HEIGHT))
        draw.line([(0, y), (w, y)], fill=(0, 0, 0, alpha))
    img = img.convert('RGBA')
    img.paste(gradient, (0, h - GRADIENT_HEIGHT), gradient)
    return img.convert('RGB')


def _add_watermark(img, text):
    img = img.copy()
    draw = ImageDraw.Draw(img, 'RGBA')
    w, h = img.size
    font = _get_font(22)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    pad_x, pad_y = 12, 8
    x = w - tw - pad_x - 16
    y = h - th - pad_y - 12
    draw.rounded_rectangle(
        [x - pad_x, y - pad_y, x + tw + pad_x, y + th + pad_y],
        radius=8, fill=(0, 0, 0, WATERMARK_OPACITY))
    draw.text((x, y), text, fill=(255, 255, 255, 240), font=font)
    return img.convert('RGB')


def _get_font(size):
    try:
        for font_name in ['arial.ttf', 'Arial.ttf', 'DejaVuSans.ttf',
                          '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
                          '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf']:
            try:
                return ImageFont.truetype(font_name, size)
            except (OSError, IOError):
                continue
    except Exception:
        pass
    return ImageFont.load_default()
