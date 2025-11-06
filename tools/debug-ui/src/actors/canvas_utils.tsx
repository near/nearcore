// Canvas utility functions and hit detection for the thread timeline visualization

export interface HitRegion {
    x: number;
    y: number;
    width: number;
    height: number;
    data: any;
    type: string;
}

/**
 * Hit detector for managing mouse interaction regions on canvas
 */
export class HitDetector {
    private regions: HitRegion[] = [];

    /**
     * Clear all hit regions
     */
    clear(): void {
        this.regions = [];
    }

    /**
     * Add a hit region for mouse interaction
     */
    addRegion(region: HitRegion): void {
        this.regions.push(region);
    }

    /**
     * Find the topmost hit region at the given coordinates
     */
    findRegion(x: number, y: number): HitRegion | null {
        // Search in reverse order (last added = topmost)
        for (let i = this.regions.length - 1; i >= 0; i--) {
            const region = this.regions[i];
            if (x >= region.x &&
                x <= region.x + region.width &&
                y >= region.y &&
                y <= region.y + region.height) {
                return region;
            }
        }
        return null;
    }
}

/**
 * Clear the entire canvas
 */
export function clearCanvas(ctx: CanvasRenderingContext2D, width: number, height: number): void {
    ctx.clearRect(0, 0, width, height);
}

/**
 * Draw a rectangle with optional fill and stroke
 * @param fillColor - Fill color (optional)
 * @param strokeColor - Stroke color (optional)
 * @param strokeWidth - Stroke width (default: 1)
 * @param opacity - Opacity (default: 1)
 */
export function drawRect(
    ctx: CanvasRenderingContext2D,
    x: number,
    y: number,
    width: number,
    height: number,
    fillColor?: string,
    strokeColor?: string,
    strokeWidth: number = 1,
    opacity: number = 1
): void {
    const prevAlpha = ctx.globalAlpha;
    ctx.globalAlpha = opacity;

    if (fillColor) {
        ctx.fillStyle = fillColor;
        ctx.fillRect(x, y, width, height);
    }

    if (strokeColor) {
        ctx.strokeStyle = strokeColor;
        ctx.lineWidth = strokeWidth;
        ctx.strokeRect(x, y, width, height);
    }

    ctx.globalAlpha = prevAlpha;
}

/**
 * Draw a line
 * @param lineCap - Line cap style (default: 'butt')
 */
export function drawLine(
    ctx: CanvasRenderingContext2D,
    x1: number,
    y1: number,
    x2: number,
    y2: number,
    color: string,
    width: number = 1,
    lineCap: CanvasLineCap = 'butt'
): void {
    ctx.strokeStyle = color;
    ctx.lineWidth = width;
    ctx.lineCap = lineCap;

    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.lineTo(x2, y2);
    ctx.stroke();
}

/**
 * Draw a rounded rectangle with optional fill and stroke
 * @param radius - Corner radius
 * @param fillColor - Fill color (optional)
 * @param strokeColor - Stroke color (optional)
 * @param strokeWidth - Stroke width (default: 1)
 * @param opacity - Opacity (default: 1)
 */
export function drawRoundedRect(
    ctx: CanvasRenderingContext2D,
    x: number,
    y: number,
    width: number,
    height: number,
    radius: number,
    fillColor?: string,
    strokeColor?: string,
    strokeWidth: number = 1,
    opacity: number = 1
): void {
    const prevAlpha = ctx.globalAlpha;
    ctx.globalAlpha = opacity;

    // Clamp radius to prevent weird shapes
    const maxRadius = Math.min(width / 2, height / 2);
    const r = Math.min(radius, maxRadius);

    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + width - r, y);
    ctx.quadraticCurveTo(x + width, y, x + width, y + r);
    ctx.lineTo(x + width, y + height - r);
    ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
    ctx.lineTo(x + r, y + height);
    ctx.quadraticCurveTo(x, y + height, x, y + height - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();

    if (fillColor) {
        ctx.fillStyle = fillColor;
        ctx.fill();
    }

    if (strokeColor) {
        ctx.strokeStyle = strokeColor;
        ctx.lineWidth = strokeWidth;
        ctx.stroke();
    }

    ctx.globalAlpha = prevAlpha;
}

/**
 * Draw text with a background rectangle
 * @param text - Text to draw
 * @param x - X position
 * @param y - Y position (baseline)
 * @param fontSize - Font size
 * @param fontFamily - Font family
 * @param textColor - Text color
 * @param backgroundColor - Background color
 * @param backgroundOpacity - Background opacity (default: 1)
 * @param fontWeight - Font weight (default: 'normal')
 * @param padding - Padding around text (default: 4)
 */
export function drawTextWithBackground(
    ctx: CanvasRenderingContext2D,
    text: string,
    x: number,
    y: number,
    fontSize: number,
    fontFamily: string,
    textColor: string,
    backgroundColor: string,
    backgroundOpacity: number = 1,
    fontWeight: string = 'normal',
    padding: number = 4
): void {
    // Set font to measure text
    ctx.font = `${fontWeight} ${fontSize}px ${fontFamily}`;
    const textMetrics = ctx.measureText(text);
    const textWidth = textMetrics.width;
    const textHeight = fontSize; // Approximation

    // Draw background
    const bgX = x - padding;
    const bgY = y - textHeight - padding;
    const bgWidth = textWidth + 2 * padding;
    const bgHeight = textHeight + 2 * padding;

    drawRect(ctx, bgX, bgY, bgWidth, bgHeight, backgroundColor, undefined, 1, backgroundOpacity);

    // Draw text
    ctx.fillStyle = textColor;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'alphabetic';
    ctx.fillText(text, x, y);
}