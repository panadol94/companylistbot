"""
Media Grid Collage Generator
Combines multiple media (photos/videos) into a single grid collage with watermark.
"""
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

# Grid config
GRID_GAP = 6          # pixels between items
GRID_BG = (20, 20, 20)  # dark background behind gaps
CORNER_RADIUS = 16     # rounded corners on each item
OUTPUT_WIDTH = 1280    # final collage width
OUTPUT_QUALITY = 92    # JPEG quality
WATERMARK_OPACITY = 180  # 0-255
GRADIENT_HEIGHT = 60   # dark gradient strip at bottom


def create_grid_collage(media_list: list, watermark_text: str = "") -> bytes:
    """
    Create a grid collage from multiple media items.
    
    Args:
        media_list: List of (bytes, media_type) tuples. media_type = 'photo'|'video'|'document'
        watermark_text: Text to display as watermark (e.g. bot name)
    
    Returns:
        JPEG bytes of the final collage image
    """
    if not media_list or len(media_list) < 2:
        # Single media or empty — no grid needed
        return None
    
    has_video = any(mt == 'video' for _, mt in media_list)
    
    # Convert all media to PIL Images
    images = []
    types = []
    for media_bytes, media_type in media_list:
        try:
            if media_type == 'photo':
                img = Image.open(BytesIO(media_bytes)).convert('RGB')
            elif media_type == 'video':
                img = _extract_video_frame(media_bytes)
            else:
                # Document — create placeholder
                img = _create_placeholder("📄 Document", (600, 400))
            images.append(img)
            types.append(media_type)
        except Exception as e:
            logger.error(f"Grid: failed to process media: {e}")
            images.append(_create_placeholder("⚠️ Error", (600, 400)))
            types.append(media_type)
    
    if len(images) < 2:
        return None
    
    # Add play icon overlay on video frames
    for i, (img, mt) in enumerate(zip(images, types)):
        if mt == 'video':
            images[i] = _add_play_icon(img)
    
    # Build grid layout
    grid = _build_grid(images)
    
    # Add dark gradient strip at bottom
    grid = _add_gradient(grid)
    
    # Add watermark
    if watermark_text:
        grid = _add_watermark(grid, watermark_text)
    
    # Export
    if has_video:
        # Convert grid image to short static video
        video_bytes = _image_to_video(grid)
        if video_bytes:
            return (video_bytes, True)
        # Fallback to photo if video conversion fails
    
    output = BytesIO()
    grid.save(output, format='JPEG', quality=OUTPUT_QUALITY, optimize=True)
    output.seek(0)
    return (output.getvalue(), False)


def _image_to_video(img: Image.Image, duration_secs: int = 3) -> bytes:
    """Convert a PIL Image to a short static MP4 video."""
    try:
        import imageio.v3 as iio
        import numpy as np
        
        # Convert PIL to numpy array
        frame = np.array(img.convert('RGB'))
        
        output = BytesIO()
        
        # Write frames (repeat the same frame for duration)
        fps = 1
        frames = [frame] * (fps * duration_secs)
        iio.imwrite(
            output, frames, 
            extension=".mp4",
            plugin="pyav",
            codec="libx264",
            fps=fps,
        )
        output.seek(0)
        return output.getvalue()
    except ImportError:
        logger.warning("imageio not available for video conversion")
    except Exception as e:
        logger.warning(f"Image to video conversion failed: {e}")
    return None


def _extract_video_frame(video_bytes: bytes) -> Image.Image:
    """Extract first frame from video bytes using imageio."""
    try:
        import imageio.v3 as iio
        # Read first frame from video bytes
        frames = iio.imread(BytesIO(video_bytes), index=0, plugin="pyav")
        return Image.fromarray(frames).convert('RGB')
    except ImportError:
        logger.warning("imageio not available, using placeholder for video")
    except Exception as e:
        logger.warning(f"Video frame extraction failed: {e}")
    
    return _create_placeholder("🎬 Video", (600, 400))


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


def _resize_to_height(img: Image.Image, target_h: int) -> Image.Image:
    """Resize image to target height while maintaining aspect ratio."""
    w, h = img.size
    if h == 0:
        return img
    ratio = target_h / h
    new_w = int(w * ratio)
    return img.resize((new_w, target_h), Image.LANCZOS)


def _resize_to_fill(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """Resize image to fit inside target dimensions, padding with dark bars (no cropping)."""
    w, h = img.size
    if w == 0 or h == 0:
        return Image.new('RGB', (target_w, target_h), GRID_BG)
    
    # Scale to fit inside target (no crop)
    scale = min(target_w / w, target_h / h)
    new_w = int(w * scale)
    new_h = int(h * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    
    # Center on dark background
    canvas = Image.new('RGB', (target_w, target_h), GRID_BG)
    x = (target_w - new_w) // 2
    y = (target_h - new_h) // 2
    canvas.paste(img, (x, y))
    return canvas


def _round_corners(img: Image.Image, radius: int) -> Image.Image:
    """Apply rounded corners to an image."""
    if radius <= 0:
        return img
    
    # Create mask with rounded rectangle
    mask = Image.new('L', img.size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle([(0, 0), img.size], radius=radius, fill=255)
    
    # Apply mask — composite over background color
    bg = Image.new('RGB', img.size, GRID_BG)
    bg.paste(img, mask=mask)
    return bg


def _build_grid(images: list) -> Image.Image:
    """Build grid layout from list of PIL Images."""
    n = len(images)
    gap = GRID_GAP
    
    if n == 2:
        # Side by side: [1][2]
        cell_w = (OUTPUT_WIDTH - gap) // 2
        cell_h = int(cell_w * 0.75)  # 4:3 ratio
        
        img1 = _round_corners(_resize_to_fill(images[0], cell_w, cell_h), CORNER_RADIUS)
        img2 = _round_corners(_resize_to_fill(images[1], cell_w, cell_h), CORNER_RADIUS)
        
        canvas = Image.new('RGB', (OUTPUT_WIDTH, cell_h), GRID_BG)
        canvas.paste(img1, (0, 0))
        canvas.paste(img2, (cell_w + gap, 0))
        return canvas
    
    elif n == 3:
        # Top row: [1][2], Bottom: [3] centered
        cell_w = (OUTPUT_WIDTH - gap) // 2
        cell_h = int(cell_w * 0.75)
        
        img1 = _round_corners(_resize_to_fill(images[0], cell_w, cell_h), CORNER_RADIUS)
        img2 = _round_corners(_resize_to_fill(images[1], cell_w, cell_h), CORNER_RADIUS)
        img3 = _round_corners(_resize_to_fill(images[2], cell_w, cell_h), CORNER_RADIUS)
        
        total_h = cell_h * 2 + gap
        canvas = Image.new('RGB', (OUTPUT_WIDTH, total_h), GRID_BG)
        canvas.paste(img1, (0, 0))
        canvas.paste(img2, (cell_w + gap, 0))
        # Center the 3rd image
        x3 = (OUTPUT_WIDTH - cell_w) // 2
        canvas.paste(img3, (x3, cell_h + gap))
        return canvas
    
    else:  # 4+
        # 2x2 grid: [1][2] / [3][4]
        cell_w = (OUTPUT_WIDTH - gap) // 2
        cell_h = int(cell_w * 0.75)
        rows = (n + 1) // 2
        total_h = cell_h * rows + gap * (rows - 1)
        
        canvas = Image.new('RGB', (OUTPUT_WIDTH, total_h), GRID_BG)
        for i, img in enumerate(images[:6]):  # Max 6 items
            row = i // 2
            col = i % 2
            x = col * (cell_w + gap)
            y = row * (cell_h + gap)
            cell = _round_corners(_resize_to_fill(img, cell_w, cell_h), CORNER_RADIUS)
            canvas.paste(cell, (x, y))
        return canvas


def _add_play_icon(img: Image.Image) -> Image.Image:
    """Add a semi-transparent play button ▶️ overlay on the image."""
    img = img.copy()
    draw = ImageDraw.Draw(img, 'RGBA')
    
    w, h = img.size
    # Circle size
    r = min(w, h) // 6
    cx, cy = w // 2, h // 2
    
    # Semi-transparent dark circle
    draw.ellipse(
        [cx - r, cy - r, cx + r, cy + r],
        fill=(0, 0, 0, 140)
    )
    
    # White triangle (play icon)
    tri_size = r * 0.6
    points = [
        (cx - tri_size * 0.4, cy - tri_size),
        (cx - tri_size * 0.4, cy + tri_size),
        (cx + tri_size * 0.8, cy),
    ]
    draw.polygon(points, fill=(255, 255, 255, 220))
    
    return img


def _add_gradient(img: Image.Image) -> Image.Image:
    """Add dark gradient strip at the bottom of the image."""
    img = img.copy()
    w, h = img.size
    gradient = Image.new('RGBA', (w, GRADIENT_HEIGHT), (0, 0, 0, 0))
    draw = ImageDraw.Draw(gradient)
    
    for y in range(GRADIENT_HEIGHT):
        alpha = int(160 * (y / GRADIENT_HEIGHT))  # 0 → 160
        draw.line([(0, y), (w, y)], fill=(0, 0, 0, alpha))
    
    # Paste gradient at bottom
    img = img.convert('RGBA')
    img.paste(gradient, (0, h - GRADIENT_HEIGHT), gradient)
    return img.convert('RGB')


def _add_watermark(img: Image.Image, text: str) -> Image.Image:
    """Add watermark text at the bottom-right corner with a subtle background."""
    img = img.copy()
    draw = ImageDraw.Draw(img, 'RGBA')
    w, h = img.size
    
    font = _get_font(22)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    
    # Position: bottom-right with padding
    pad_x, pad_y = 12, 8
    x = w - tw - pad_x - 16
    y = h - th - pad_y - 12
    
    # Semi-transparent background pill
    draw.rounded_rectangle(
        [x - pad_x, y - pad_y, x + tw + pad_x, y + th + pad_y],
        radius=8,
        fill=(0, 0, 0, WATERMARK_OPACITY)
    )
    
    # White text
    draw.text((x, y), text, fill=(255, 255, 255, 240), font=font)
    
    return img.convert('RGB')


def _get_font(size: int):
    """Get a font, falling back to default if not available."""
    try:
        # Try common fonts
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
