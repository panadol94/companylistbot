"""
Media Grid Collage Generator
Combines multiple media (photos/videos) into a single grid collage.
Features: fade-in, branding bar, golden border, company name, media badge, animated watermark.
Videos use FFmpeg subprocess — fast, low memory.
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
GRID_BG_HEX = '0x141414'
GRID_BG = (20, 20, 20)
CORNER_RADIUS = 16
OUTPUT_WIDTH = 1280
OUTPUT_QUALITY = 92
WATERMARK_OPACITY = 180
BRANDING_BAR_H = 56     # bottom branding bar height
BORDER_W = 3             # golden border width
BORDER_COLOR = 'gold'
BORDER_COLOR_RGB = (218, 165, 32)


def create_grid_collage(media_list: list, watermark_text: str = "", company_name: str = ""):
    """
    Create a grid collage from multiple media items.
    Returns (bytes, is_video) tuple or None.
    """
    if not media_list or len(media_list) < 2:
        return None
    
    has_video = any(mt == 'video' for _, mt in media_list)
    
    # Count media types for badge
    photo_count = sum(1 for _, mt in media_list if mt == 'photo')
    video_count = sum(1 for _, mt in media_list if mt == 'video')
    
    if has_video:
        return _create_mosaic_video_ffmpeg(media_list, watermark_text, company_name, photo_count, video_count)
    else:
        return _create_static_grid(media_list, watermark_text, company_name, photo_count, video_count)


def _create_static_grid(media_list, watermark_text, company_name, photo_count, video_count):
    """Create static JPEG grid for photos only — with all visual enhancements."""
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
    
    # Add branding bar at bottom
    grid = _add_branding_bar(grid, watermark_text, company_name, photo_count, video_count)
    
    # Add golden border
    grid = _add_border(grid)
    
    output = BytesIO()
    grid.save(output, format='JPEG', quality=OUTPUT_QUALITY, optimize=True)
    output.seek(0)
    return (output.getvalue(), False)


def _add_branding_bar(img, watermark_text, company_name, photo_count, video_count):
    """Add a dark branding bar at bottom with bot name, company name, media badge."""
    w, h = img.size
    new_h = h + BRANDING_BAR_H
    canvas = Image.new('RGB', (w, new_h), GRID_BG)
    canvas.paste(img, (0, 0))
    
    draw = ImageDraw.Draw(canvas, 'RGBA')
    
    # Dark bar background
    draw.rectangle(
        [(0, h), (w, new_h)],
        fill=(15, 15, 15, 255)
    )
    
    # Subtle top border on bar
    draw.line([(0, h), (w, h)], fill=BORDER_COLOR_RGB, width=1)
    
    font_brand = _get_font(20)
    font_small = _get_font(14)
    bar_center_y = h + (BRANDING_BAR_H // 2)
    
    # Left: Media count badge
    badge_parts = []
    if photo_count > 0:
        badge_parts.append(f"📷 {photo_count}")
    if video_count > 0:
        badge_parts.append(f"🎥 {video_count}")
    badge_text = " + ".join(badge_parts) if badge_parts else ""
    if badge_text:
        bbox = draw.textbbox((0, 0), badge_text, font=font_small)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        bx, by = 14, bar_center_y - th // 2
        draw.rounded_rectangle(
            [bx - 6, by - 4, bx + tw + 6, by + th + 4],
            radius=6, fill=(255, 255, 255, 30))
        draw.text((bx, by), badge_text, fill=(200, 200, 200, 230), font=font_small)
    
    # Center: Company name
    if company_name:
        bbox = draw.textbbox((0, 0), company_name, font=font_brand)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        cx = (w - tw) // 2
        cy = bar_center_y - th // 2
        draw.text((cx, cy), company_name, fill=(255, 255, 255, 240), font=font_brand)
    
    # Right: Watermark / bot name
    if watermark_text:
        bbox = draw.textbbox((0, 0), watermark_text, font=font_brand)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        wx = w - tw - 14
        wy = bar_center_y - th // 2
        draw.text((wx, wy), watermark_text, fill=(*BORDER_COLOR_RGB, 240), font=font_brand)
    
    return canvas.convert('RGB')


def _add_border(img):
    """Add thin golden border around the entire image."""
    w, h = img.size
    draw = ImageDraw.Draw(img)
    for i in range(BORDER_W):
        draw.rectangle(
            [(i, i), (w - 1 - i, h - 1 - i)],
            outline=BORDER_COLOR_RGB
        )
    return img


def _create_mosaic_video_ffmpeg(media_list, watermark_text, company_name, photo_count, video_count):
    """
    Create mosaic video using FFmpeg subprocess with visual enhancements:
    fade-in, branding bar, golden border, company name, media badge, animated watermark.
    """
    n = min(len(media_list), 6)
    gap = GRID_GAP
    
    cell_w = (OUTPUT_WIDTH - gap) // 2
    cell_w = cell_w - (cell_w % 2)
    cell_h = int(cell_w * 0.75)
    cell_h = cell_h - (cell_h % 2)
    
    if n == 2:
        rows = 1
    elif n == 3:
        rows = 2
    else:
        rows = (n + 1) // 2
    
    grid_w = cell_w * 2 + gap
    grid_h = cell_h * rows + gap * (rows - 1)
    
    # Total canvas = grid + branding bar + borders
    canvas_w = grid_w + (BORDER_W * 2)
    canvas_h = grid_h + BRANDING_BAR_H + (BORDER_W * 2)
    canvas_w = canvas_w + (canvas_w % 2)  # ensure even
    canvas_h = canvas_h + (canvas_h % 2)
    
    temp_files = []
    
    try:
        inputs = []
        max_duration = 0
        
        for idx, (media_bytes, media_type) in enumerate(media_list[:6]):
            suffix = '.mp4' if media_type == 'video' else '.jpg'
            tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
            tmp.write(media_bytes)
            tmp.close()
            temp_files.append(tmp.name)
            inputs.append({'path': tmp.name, 'type': media_type})
            
            if media_type == 'video':
                dur = _get_video_duration(tmp.name)
                if dur > max_duration:
                    max_duration = dur
        
        if max_duration <= 0:
            max_duration = 10
        
        positions = _get_cell_positions(n, cell_w, cell_h, gap)
        
        # Build FFmpeg command
        cmd = ['ffmpeg', '-y']
        
        # Input 0: background
        cmd.extend([
            '-f', 'lavfi', '-i',
            f'color=c={GRID_BG_HEX}:s={canvas_w}x{canvas_h}:d={max_duration}:r=30'
        ])
        
        first_video_input = None
        for i, inp in enumerate(inputs):
            if inp['type'] != 'video':
                cmd.extend(['-loop', '1', '-t', str(max_duration)])
            else:
                if first_video_input is None:
                    first_video_input = i + 1
            cmd.extend(['-i', inp['path']])
        
        # Build filter_complex
        filters = []
        
        # Scale each input to cell size
        for i in range(n):
            input_idx = i + 1
            filters.append(
                f'[{input_idx}:v]scale={cell_w}:{cell_h}:'
                f'force_original_aspect_ratio=decrease,'
                f'pad={cell_w}:{cell_h}:(ow-iw)/2:(oh-ih)/2:'
                f'color={GRID_BG_HEX},setsar=1[v{i}]'
            )
        
        # Overlay cells onto background (offset by border width)
        prev = '0:v'
        for i in range(n):
            x, y = positions[i]
            x += BORDER_W  # offset for border
            y += BORDER_W
            out_label = f'tmp{i}'
            filters.append(
                f'[{prev}][v{i}]overlay={x}:{y}:shortest=1[{out_label}]'
            )
            prev = out_label
        
        # 1. Golden border (drawbox)
        filters.append(
            f'[{prev}]drawbox=x=0:y=0:w=iw:h=ih:color={BORDER_COLOR}:t={BORDER_W}[bordered]'
        )
        prev = 'bordered'
        
        # 2. Branding bar background (dark strip at bottom)
        bar_y = BORDER_W + grid_h
        filters.append(
            f'[{prev}]drawbox=x=0:y={bar_y}:w=iw:h={BRANDING_BAR_H}:'
            f'color=black@0.9:t=fill[bar]'
        )
        # Gold line above bar
        filters.append(
            f'[bar]drawbox=x=0:y={bar_y}:w=iw:h=1:color={BORDER_COLOR}:t=fill[barline]'
        )
        prev = 'barline'
        
        # 3. Media count badge (top-left of bar)
        badge_parts = []
        if photo_count > 0:
            badge_parts.append(f"{photo_count} Photo{'s' if photo_count > 1 else ''}")
        if video_count > 0:
            badge_parts.append(f"{video_count} Video{'s' if video_count > 1 else ''}")
        badge_text = " + ".join(badge_parts)
        if badge_text:
            safe_badge = badge_text.replace("'", "\\'").replace(":", "\\:")
            badge_y = bar_y + (BRANDING_BAR_H // 2)
            filters.append(
                f'[{prev}]drawtext=text=\'{safe_badge}\':'
                f'x=16:y={badge_y}-th/2:'
                f'fontsize=15:fontcolor=white@0.7:'
                f'box=1:boxcolor=white@0.1:boxborderw=5[badge]'
            )
            prev = 'badge'
        
        # 4. Company name (center of bar)
        if company_name:
            safe_company = company_name.replace("'", "\\'").replace(":", "\\:")
            company_y = bar_y + (BRANDING_BAR_H // 2)
            filters.append(
                f'[{prev}]drawtext=text=\'{safe_company}\':'
                f'x=(w-tw)/2:y={company_y}-th/2:'
                f'fontsize=22:fontcolor=white[company]'
            )
            prev = 'company'
        
        # 5. Animated watermark (slides in from right, 0.3s delay)
        if watermark_text:
            safe_wm = watermark_text.replace("'", "\\'").replace(":", "\\:")
            wm_y = bar_y + (BRANDING_BAR_H // 2)
            # Slide from right edge to final position over 0.5s, starting at 0.3s
            filters.append(
                f'[{prev}]drawtext=text=\'{safe_wm}\':'
                f'x=\'if(lt(t\\,0.8)\\,w\\,min(w-tw-16\\,w-(w-tw-16+tw)*min(1\\,(t-0.3)*3)))\'  :'
                f'y={wm_y}-th/2:'
                f'fontsize=20:fontcolor={BORDER_COLOR}[watermark]'
            )
            prev = 'watermark'
        
        # 6. Fade-in (0.5s from black)
        filters.append(
            f'[{prev}]fade=t=in:st=0:d=0.5[out]'
        )
        final_label = '[out]'
        
        filter_str = ';'.join(filters)
        
        # Output file
        out_tmp = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False)
        out_tmp.close()
        temp_files.append(out_tmp.name)
        
        cmd.extend([
            '-filter_complex', filter_str,
            '-map', final_label,
        ])
        if first_video_input is not None:
            cmd.extend(['-map', f'{first_video_input}:a?'])
            cmd.extend(['-c:a', 'aac', '-b:a', '128k'])
        cmd.extend([
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-crf', '23',
            '-pix_fmt', 'yuv420p',
            '-t', str(max_duration),
            '-movflags', '+faststart',
            out_tmp.name
        ])
        
        logger.info(f"FFmpeg grid: {n} inputs, {max_duration:.1f}s, {canvas_w}x{canvas_h}")
        
        timeout = max(max_duration * 3, 120)
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        
        if result.returncode != 0:
            logger.error(f"FFmpeg failed (rc={result.returncode}): {result.stderr[-500:]}")
            return None
        
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
    """Get video duration in seconds."""
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


# --- Static grid helpers (PIL) ---

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
