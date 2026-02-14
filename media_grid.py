"""
Media Grid Collage Generator
Combines multiple media (photos/videos) into a single grid collage with watermark.
Videos play frame-by-frame in the grid mosaic. Memory-efficient streaming approach.
"""
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import logging
import numpy as np
import tempfile
import os

logger = logging.getLogger(__name__)

# Grid config
GRID_GAP = 6
GRID_BG = (20, 20, 20)
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
        return _create_mosaic_video(media_list, watermark_text)
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


def _create_mosaic_video(media_list, watermark_text):
    """
    Create mosaic video — videos play, photos stay static.
    STREAMING approach: read 1 frame → composite → write → discard.
    Never holds all frames in memory.
    """
    try:
        import imageio.v3 as iio
        import av
    except ImportError:
        logger.error("imageio/av not available for mosaic video")
        return None
    
    n = min(len(media_list), 6)
    gap = GRID_GAP
    cell_w = (OUTPUT_WIDTH - gap) // 2
    cell_h = int(cell_w * 0.75)
    
    # Canvas size
    if n == 2:
        canvas_w, canvas_h = OUTPUT_WIDTH, cell_h
    elif n == 3:
        canvas_w, canvas_h = OUTPUT_WIDTH, cell_h * 2 + gap
    else:
        rows = (n + 1) // 2
        canvas_w, canvas_h = OUTPUT_WIDTH, cell_h * rows + gap * (rows - 1)
    
    final_h = canvas_h
    positions = _get_cell_positions(n, cell_w, cell_h, gap)
    
    # Prepare cells: static images (numpy) and video file paths for streaming
    static_cells = {}     # cell_idx -> numpy array (pre-rendered)
    video_cells = {}      # cell_idx -> {'tmpfile': path, 'bytes': bytes}
    video_fps = 30        # default
    total_video_frames = 0
    
    temp_files = []  # Track temp files for cleanup
    
    try:
        for idx, (media_bytes, media_type) in enumerate(media_list[:6]):
            if media_type == 'video':
                # Save video bytes to temp file for streaming read
                tmp = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False)
                tmp.write(media_bytes)
                tmp.close()
                temp_files.append(tmp.name)
                
                # Get FPS and frame count from video
                try:
                    container = av.open(tmp.name)
                    stream = container.streams.video[0]
                    if stream.average_rate:
                        video_fps = float(stream.average_rate)
                    frame_count = stream.frames or 0
                    if frame_count == 0:
                        # Estimate from duration
                        if stream.duration and stream.time_base:
                            dur_secs = float(stream.duration * stream.time_base)
                            frame_count = int(dur_secs * video_fps)
                    total_video_frames = max(total_video_frames, frame_count)
                    container.close()
                except Exception as e:
                    logger.warning(f"Could not get video info: {e}")
                
                video_cells[idx] = {'tmpfile': tmp.name}
            else:
                # Photo — pre-render as static cell
                try:
                    img = Image.open(BytesIO(media_bytes)).convert('RGB')
                except Exception:
                    img = _create_placeholder("⚠️", (600, 400))
                img = _resize_to_fill(img, cell_w, cell_h)
                img = _round_corners(img, CORNER_RADIUS)
                static_cells[idx] = np.array(img)
        
        if total_video_frames == 0:
            total_video_frames = 300  # fallback: ~10s
        
        # Pre-compute overlay (gradient + watermark) as RGBA numpy
        overlay_img = Image.new('RGBA', (canvas_w, final_h), (0, 0, 0, 0))
        draw_ov = ImageDraw.Draw(overlay_img)
        # Gradient at bottom
        grad_start = max(0, final_h - GRADIENT_HEIGHT)
        for y in range(GRADIENT_HEIGHT):
            if grad_start + y >= final_h:
                break
            alpha = int(160 * (y / GRADIENT_HEIGHT))
            draw_ov.line([(0, grad_start + y), (canvas_w, grad_start + y)], fill=(0, 0, 0, alpha))
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
        
        overlay_arr = np.array(overlay_img)
        overlay_alpha = overlay_arr[:, :, 3:4].astype(np.float32) / 255.0
        overlay_rgb = overlay_arr[:, :, :3].astype(np.float32)
        
        bg_arr = np.full((final_h, canvas_w, 3), GRID_BG, dtype=np.uint8)
        
        # Open all video readers for streaming
        video_readers = {}
        for idx, vc in video_cells.items():
            try:
                video_readers[idx] = av.open(vc['tmpfile'])
            except Exception as e:
                logger.error(f"Failed to open video reader {idx}: {e}")
        
        # Create video iterators
        video_iters = {}
        for idx, reader in video_readers.items():
            video_iters[idx] = reader.decode(video=0)
        
        # Cache last frame per video cell (for when video ends or for looping)
        last_frames = {}
        
        # Output: write to temp file to avoid holding all frames in memory
        out_tmp = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False)
        out_tmp.close()
        temp_files.append(out_tmp.name)
        
        output_container = av.open(out_tmp.name, mode='w')
        output_stream = output_container.add_stream('libx264', rate=int(video_fps))
        output_stream.width = canvas_w
        output_stream.height = final_h
        output_stream.pix_fmt = 'yuv420p'
        # Ensure even dimensions for h264
        if output_stream.width % 2 != 0:
            output_stream.width -= 1
        if output_stream.height % 2 != 0:
            output_stream.height -= 1
        
        logger.info(f"Grid mosaic: {n} cells, ~{total_video_frames} frames @ {video_fps}fps, streaming")
        
        frame_count = 0
        for frame_idx in range(total_video_frames):
            canvas = bg_arr.copy()
            
            # Place each cell
            for cell_idx in range(n):
                if cell_idx >= len(positions):
                    break
                x, y = positions[cell_idx]
                
                if cell_idx in video_iters:
                    # Get next video frame
                    try:
                        av_frame = next(video_iters[cell_idx])
                        pil_frame = av_frame.to_image().convert('RGB')
                        pil_frame = _resize_to_fill(pil_frame, cell_w, cell_h)
                        pil_frame = _round_corners(pil_frame, CORNER_RADIUS)
                        cell_arr = np.array(pil_frame)
                        last_frames[cell_idx] = cell_arr
                    except StopIteration:
                        # Video ended — use last frame
                        cell_arr = last_frames.get(cell_idx)
                        if cell_arr is None:
                            continue
                    except Exception:
                        cell_arr = last_frames.get(cell_idx)
                        if cell_arr is None:
                            continue
                elif cell_idx in static_cells:
                    cell_arr = static_cells[cell_idx]
                else:
                    continue
                
                # Paste cell
                ch, cw = cell_arr.shape[:2]
                y_end = min(y + ch, final_h)
                x_end = min(x + cw, canvas_w)
                canvas[y:y_end, x:x_end] = cell_arr[:y_end-y, :x_end-x]
            
            # Apply overlay
            canvas_f = canvas.astype(np.float32)
            blended = (canvas_f * (1 - overlay_alpha) + overlay_rgb * overlay_alpha).astype(np.uint8)
            
            # Write frame directly to output (no accumulation)
            av_out_frame = av.VideoFrame.from_ndarray(blended, format='rgb24')
            for packet in output_stream.encode(av_out_frame):
                output_container.mux(packet)
            
            frame_count += 1
        
        # Flush
        for packet in output_stream.encode():
            output_container.mux(packet)
        output_container.close()
        
        # Close readers
        for reader in video_readers.values():
            try:
                reader.close()
            except Exception:
                pass
        
        # Read output file
        with open(out_tmp.name, 'rb') as f:
            result_bytes = f.read()
        
        logger.info(f"Grid mosaic done: {frame_count} frames, {len(result_bytes)} bytes")
        return (result_bytes, True)
    
    except Exception as e:
        logger.error(f"Mosaic video failed: {e}")
        return None
    finally:
        # Cleanup temp files
        for tf in temp_files:
            try:
                os.unlink(tf)
            except Exception:
                pass


def _get_cell_positions(n, cell_w, cell_h, gap):
    """Get (x, y) position for each cell in the grid."""
    if n == 2:
        return [(0, 0), (cell_w + gap, 0)]
    elif n == 3:
        return [(0, 0), (cell_w + gap, 0),
                ((OUTPUT_WIDTH - cell_w) // 2, cell_h + gap)]
    else:
        positions = []
        for i in range(min(n, 6)):
            row = i // 2
            col = i % 2
            positions.append((col * (cell_w + gap), row * (cell_h + gap)))
        return positions


def _create_placeholder(text: str, size: tuple) -> Image.Image:
    img = Image.new('RGB', size, (60, 60, 70))
    draw = ImageDraw.Draw(img)
    font = _get_font(32)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(((size[0]-tw)//2, (size[1]-th)//2), text, fill=(200,200,200), font=font)
    return img


def _resize_to_fill(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """Resize image to fit inside target, padding with dark bars (no crop)."""
    w, h = img.size
    if w == 0 or h == 0:
        return Image.new('RGB', (target_w, target_h), GRID_BG)
    scale = min(target_w / w, target_h / h)
    new_w, new_h = int(w * scale), int(h * scale)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    canvas = Image.new('RGB', (target_w, target_h), GRID_BG)
    canvas.paste(img, ((target_w-new_w)//2, (target_h-new_h)//2))
    return canvas


def _round_corners(img: Image.Image, radius: int) -> Image.Image:
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
            row, col = i // 2, i % 2
            cell = _round_corners(_resize_to_fill(img, cell_w, cell_h), CORNER_RADIUS)
            canvas.paste(cell, (col * (cell_w + gap), row * (cell_h + gap)))
        return canvas


def _add_gradient(img: Image.Image) -> Image.Image:
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
