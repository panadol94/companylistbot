"""
Media Grid Collage Generator
Combines multiple media (photos/videos) into a single grid collage with watermark.
Videos play frame-by-frame in the grid mosaic.
"""
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import logging
import numpy as np

logger = logging.getLogger(__name__)

# Grid config
GRID_GAP = 6          # pixels between items
GRID_BG = (20, 20, 20)  # dark background behind gaps
CORNER_RADIUS = 16     # rounded corners on each item
OUTPUT_WIDTH = 1280    # final collage width
OUTPUT_QUALITY = 92    # JPEG quality
WATERMARK_OPACITY = 180  # 0-255
GRADIENT_HEIGHT = 60   # dark gradient strip at bottom


def create_grid_collage(media_list: list, watermark_text: str = ""):
    """
    Create a grid collage from multiple media items.
    
    Args:
        media_list: List of (bytes, media_type) tuples
        watermark_text: Text to display as watermark (e.g. bot name)
    
    Returns:
        (bytes, is_video) tuple or None
    """
    if not media_list or len(media_list) < 2:
        return None
    
    has_video = any(mt == 'video' for _, mt in media_list)
    
    if has_video:
        # Create mosaic video with playing video cells
        return _create_mosaic_video(media_list, watermark_text)
    else:
        # All photos — create static grid image
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


def _create_mosaic_video(media_list, watermark_text):
    """Create mosaic video — videos play, photos stay static."""
    try:
        import imageio.v3 as iio
    except ImportError:
        logger.error("imageio not available for mosaic video")
        return None
    
    n = len(media_list)
    gap = GRID_GAP
    
    # Calculate cell dimensions
    cell_w = (OUTPUT_WIDTH - gap) // 2
    cell_h = int(cell_w * 0.75)
    
    # Compute canvas size
    if n == 2:
        canvas_w, canvas_h = OUTPUT_WIDTH, cell_h
    elif n == 3:
        canvas_w, canvas_h = OUTPUT_WIDTH, cell_h * 2 + gap
    else:
        rows = (min(n, 6) + 1) // 2
        canvas_w, canvas_h = OUTPUT_WIDTH, cell_h * rows + gap * (rows - 1)
    
    # Add space for gradient at bottom
    final_h = canvas_h + GRADIENT_HEIGHT
    
    # Pre-process each media item
    cells = []  # Each cell: {'type': 'photo'|'video', 'static': PIL or None, 'frames': list or None}
    max_frames = 0
    source_fps = 30  # default, will be overridden by actual video fps
    
    for media_bytes, media_type in media_list[:6]:  # Max 6 items
        if media_type == 'video':
            try:
                # Detect original FPS
                try:
                    props = iio.improps(BytesIO(media_bytes), plugin="pyav")
                    if hasattr(props, 'fps') and props.fps:
                        source_fps = props.fps
                except Exception:
                    pass
                
                frames_raw = iio.imread(BytesIO(media_bytes), plugin="pyav")
                # Convert & resize frames
                video_frames = []
                for frame_arr in frames_raw:
                    frame_img = Image.fromarray(frame_arr).convert('RGB')
                    frame_img = _resize_to_fill(frame_img, cell_w, cell_h)
                    frame_img = _round_corners(frame_img, CORNER_RADIUS)
                    video_frames.append(np.array(frame_img))
                
                if video_frames:
                    cells.append({'type': 'video', 'frames': video_frames, 'static': None})
                    max_frames = max(max_frames, len(video_frames))
                else:
                    # No frames decoded, use placeholder
                    ph = _round_corners(_resize_to_fill(
                        _create_placeholder("🎬", (600, 400)), cell_w, cell_h), CORNER_RADIUS)
                    cells.append({'type': 'photo', 'static': np.array(ph), 'frames': None})
            except Exception as e:
                logger.error(f"Video frame read failed: {e}")
                ph = _round_corners(_resize_to_fill(
                    _create_placeholder("🎬", (600, 400)), cell_w, cell_h), CORNER_RADIUS)
                cells.append({'type': 'photo', 'static': np.array(ph), 'frames': None})
        else:
            try:
                img = Image.open(BytesIO(media_bytes)).convert('RGB')
            except Exception:
                img = _create_placeholder("⚠️", (600, 400))
            img = _resize_to_fill(img, cell_w, cell_h)
            img = _round_corners(img, CORNER_RADIUS)
            cells.append({'type': 'photo', 'static': np.array(img), 'frames': None})
    
    if max_frames == 0:
        # No video frames could be read — fallback to static
        return _create_static_grid(media_list, watermark_text)
    
    # Pre-compute cell positions
    positions = _get_cell_positions(len(cells), cell_w, cell_h, gap)
    
    # Pre-render watermark + gradient overlay
    overlay_img = Image.new('RGBA', (canvas_w, final_h), (0, 0, 0, 0))
    # Gradient at bottom
    draw_ov = ImageDraw.Draw(overlay_img)
    grad_y_start = canvas_h
    for y in range(GRADIENT_HEIGHT):
        alpha = int(160 * (y / GRADIENT_HEIGHT))
        draw_ov.line([(0, grad_y_start + y), (canvas_w, grad_y_start + y)], fill=(0, 0, 0, alpha))
    # Watermark
    if watermark_text:
        font = _get_font(22)
        bbox = draw_ov.textbbox((0, 0), watermark_text, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        pad_x, pad_y = 12, 8
        wx = canvas_w - tw - pad_x - 16
        wy = final_h - th - pad_y - 12
        draw_ov.rounded_rectangle(
            [wx - pad_x, wy - pad_y, wx + tw + pad_x, wy + th + pad_y],
            radius=8, fill=(0, 0, 0, WATERMARK_OPACITY))
        draw_ov.text((wx, wy), watermark_text, fill=(255, 255, 255, 240), font=font)
    
    overlay_arr = np.array(overlay_img)  # RGBA
    
    # Generate frames
    logger.info(f"Grid mosaic: {len(cells)} cells, {max_frames} frames @ {source_fps}fps")
    
    output_frames = []
    bg_arr = np.full((final_h, canvas_w, 3), GRID_BG, dtype=np.uint8)
    
    for frame_idx in range(max_frames):
        canvas = bg_arr.copy()
        
        for cell_idx, cell in enumerate(cells):
            if cell_idx >= len(positions):
                break
            x, y = positions[cell_idx]
            
            if cell['type'] == 'video' and cell['frames']:
                # Use current video frame (loop if shorter)
                vidx = frame_idx % len(cell['frames'])
                cell_frame = cell['frames'][vidx]
            else:
                cell_frame = cell['static']
            
            if cell_frame is not None:
                ch, cw = cell_frame.shape[:2]
                # Paste cell onto canvas
                y_end = min(y + ch, final_h)
                x_end = min(x + cw, canvas_w)
                canvas[y:y_end, x:x_end] = cell_frame[:y_end-y, :x_end-x]
        
        # Apply overlay (gradient + watermark)
        # Blend RGBA overlay onto RGB canvas
        alpha = overlay_arr[:, :, 3:4].astype(np.float32) / 255.0
        overlay_rgb = overlay_arr[:, :, :3].astype(np.float32)
        canvas_f = canvas.astype(np.float32)
        blended = (canvas_f * (1 - alpha) + overlay_rgb * alpha).astype(np.uint8)
        
        output_frames.append(blended)
    
    if not output_frames:
        return None
    
    logger.info(f"Grid mosaic: writing {len(output_frames)} frames as MP4 @ {source_fps}fps")
    
    try:
        out_buf = BytesIO()
        iio.imwrite(
            out_buf,
            output_frames,
            extension=".mp4",
            plugin="pyav",
            codec="libx264",
            fps=source_fps,
        )
        out_buf.seek(0)
        return (out_buf.getvalue(), True)
    except Exception as e:
        logger.error(f"Mosaic video write failed: {e}")
        return None


def _get_cell_positions(n, cell_w, cell_h, gap):
    """Get (x, y) position for each cell in the grid."""
    positions = []
    if n == 2:
        positions = [(0, 0), (cell_w + gap, 0)]
    elif n == 3:
        positions = [(0, 0), (cell_w + gap, 0),
                     ((OUTPUT_WIDTH - cell_w) // 2, cell_h + gap)]
    else:
        for i in range(min(n, 6)):
            row = i // 2
            col = i % 2
            positions.append((col * (cell_w + gap), row * (cell_h + gap)))
    return positions


def _create_placeholder(text: str, size: tuple) -> Image.Image:
    """Create a gray placeholder image with text."""
    img = Image.new('RGB', size, (60, 60, 70))
    draw = ImageDraw.Draw(img)
    font = _get_font(32)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    x = (size[0] - tw) // 2
    y = (size[1] - th) // 2
    draw.text((x, y), text, fill=(200, 200, 200), font=font)
    return img


def _resize_to_fill(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """Resize image to fit inside target dimensions, padding with dark bars."""
    w, h = img.size
    if w == 0 or h == 0:
        return Image.new('RGB', (target_w, target_h), GRID_BG)
    scale = min(target_w / w, target_h / h)
    new_w = int(w * scale)
    new_h = int(h * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    canvas = Image.new('RGB', (target_w, target_h), GRID_BG)
    x = (target_w - new_w) // 2
    y = (target_h - new_h) // 2
    canvas.paste(img, (x, y))
    return canvas


def _round_corners(img: Image.Image, radius: int) -> Image.Image:
    """Apply rounded corners to an image."""
    if radius <= 0:
        return img
    mask = Image.new('L', img.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle([(0, 0), img.size], radius=radius, fill=255)
    bg = Image.new('RGB', img.size, GRID_BG)
    bg.paste(img, mask=mask)
    return bg


def _build_grid(images: list) -> Image.Image:
    """Build static grid layout from list of PIL Images."""
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
            row = i // 2
            col = i % 2
            x = col * (cell_w + gap)
            y = row * (cell_h + gap)
            cell = _round_corners(_resize_to_fill(img, cell_w, cell_h), CORNER_RADIUS)
            canvas.paste(cell, (x, y))
        return canvas


def _add_gradient(img: Image.Image) -> Image.Image:
    """Add dark gradient strip at the bottom."""
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


def _add_watermark(img: Image.Image, text: str) -> Image.Image:
    """Add watermark text at the bottom-right corner."""
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


def _get_font(size: int):
    """Get a font, falling back to default if not available."""
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
