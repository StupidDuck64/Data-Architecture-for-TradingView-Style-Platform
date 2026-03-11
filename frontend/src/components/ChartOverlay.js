import React, { useState, useRef, useCallback, useEffect } from 'react';

const SNAP_RADIUS = 14;    // px — snaps to existing drawing endpoints
const MAG_RADIUS  = 18;    // px — magnetic snap to candle OHLC points

// Tools that require sequential multi-click (not drag)
const MULTI_CLICK_NEEDED = { elliottWave: true, harmonicABCD: true };

// ─── Helpers ──────────────────────────────────────────────────────
function dashToDashArray(style) {
  if (style === 'dashed') return '8 4';
  if (style === 'dotted') return '3 3';
  return undefined;
}

function magneticSnap(pos, candlePixels) {
  if (!candlePixels || candlePixels.length === 0) return pos;
  let best = null, bestDist = MAG_RADIUS;
  for (const cp of candlePixels) {
    for (const y of [cp.openY, cp.highY, cp.lowY, cp.closeY]) {
      const dx = cp.x - pos.x, dy = y - pos.y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < bestDist) { bestDist = dist; best = { x: cp.x, y }; }
    }
  }
  return best || pos;
}

const ELLIOTT_IMPULSE_LABELS  = ['0','1','2','3','4','5'];
const ELLIOTT_CORRECT_LABELS  = ['0','A','B','C'];

function renderElliottWave(d, isPreview) {
  const { points, settings = {} } = d;
  if (!points || points.length < 2) return null;
  const color     = settings.color     || '#f97316';
  const lw        = settings.lineWidth || 2;
  const showLabel = settings.showLabel !== false;
  const waveType  = settings.waveType  || 'impulse';
  const labels    = waveType === 'corrective' ? ELLIOTT_CORRECT_LABELS : ELLIOTT_IMPULSE_LABELS;
  return (
    <g key={d.id} opacity={isPreview ? 0.65 : 1}>
      {points.slice(0, -1).map((pt, i) => (
        <line key={`seg-${i}`} x1={pt.x} y1={pt.y} x2={points[i+1].x} y2={points[i+1].y}
          stroke={color} strokeWidth={lw} strokeLinejoin="round" />
      ))}
      {points.map((pt, i) => (
        <g key={`node-${i}`}>
          <circle cx={pt.x} cy={pt.y} r="4" fill={color} />
          {showLabel && (
            <text x={pt.x} y={pt.y - 8} textAnchor="middle" fontSize="11" fill={color} fontWeight="bold">
              {labels[i] || i}
            </text>
          )}
        </g>
      ))}
    </g>
  );
}

function renderHarmonicABCD(d, isPreview) {
  const { points, settings = {} } = d;
  if (!points || points.length < 2) return null;
  const color      = settings.color     || '#a855f7';
  const lw         = settings.lineWidth || 2;
  const showLabel  = settings.showLabel !== false;
  const fiboLevels = settings.fiboLevels || [0.618, 1.272];
  const labels     = ['A','B','C','D'];
  const pairs      = [[0,1],[1,2],[2,3],[0,3]];
  const AB = points.length >= 2 ? Math.sqrt((points[1].x-points[0].x)**2+(points[1].y-points[0].y)**2) : 0;
  const ratioBC = points.length >= 3 && AB > 0
    ? (Math.sqrt((points[2].x-points[1].x)**2+(points[2].y-points[1].y)**2)/AB).toFixed(3) : null;
  const ratioCD = points.length >= 4 && AB > 0
    ? (Math.sqrt((points[3].x-points[2].x)**2+(points[3].y-points[2].y)**2)/AB).toFixed(3) : null;
  return (
    <g key={d.id} opacity={isPreview ? 0.65 : 1}>
      {pairs.map(([from, to]) => {
        if (!points[from] || !points[to]) return null;
        const isDiag = from === 0 && to === 3;
        return (
          <line key={`${from}-${to}`}
            x1={points[from].x} y1={points[from].y} x2={points[to].x} y2={points[to].y}
            stroke={color} strokeWidth={isDiag ? lw * 0.6 : lw}
            strokeDasharray={isDiag ? '6 3' : undefined} opacity={isDiag ? 0.5 : 1} />
        );
      })}
      {points.length >= 3 && showLabel && ratioBC && (
        <text x={(points[1].x+points[2].x)/2+6} y={(points[1].y+points[2].y)/2} fontSize="10" fill={color} opacity="0.85">{ratioBC}</text>
      )}
      {points.length >= 4 && showLabel && ratioCD && (
        <text x={(points[2].x+points[3].x)/2+6} y={(points[2].y+points[3].y)/2} fontSize="10" fill={color} opacity="0.85">{ratioCD}</text>
      )}
      {points.length >= 4 && showLabel && (
        <g>
          <rect x={points[3].x+6} y={points[3].y-10} width={46} height={14} rx="3" fill="rgba(0,0,0,0.65)" />
          <text x={points[3].x+8} y={points[3].y+1} fontSize="10" fill={color}>
            {fiboLevels.map(l => (l*100).toFixed(0)+'%').join(' / ')}
          </text>
        </g>
      )}
      {points.map((pt, i) => (
        <g key={`n-${i}`}>
          <circle cx={pt.x} cy={pt.y} r="4.5" fill={color} />
          {showLabel && (
            <text x={pt.x} y={pt.y-9} textAnchor="middle" fontSize="12" fontWeight="bold" fill={color}>
              {labels[i] || i}
            </text>
          )}
        </g>
      ))}
    </g>
  );
}

// ─── Main component ───────────────────────────────────────────────
const ChartOverlay = ({ activeTool, drawings, onAddDrawing, toolSettings, candlePixels }) => {
  const svgRef = useRef(null);
  const [isDrawing,    setIsDrawing]    = useState(false);
  const [startPoint,   setStartPoint]   = useState(null);
  const [currentPoint, setCurrentPoint] = useState(null);
  const [multiPoints,  setMultiPoints]  = useState([]);
  const [textInput, setTextInput] = useState(null);

  const isMultiClick = MULTI_CLICK_NEEDED[activeTool] || false;

  const activeSettings = useCallback(() => {
    return (toolSettings && toolSettings[activeTool]) || {};
  }, [toolSettings, activeTool]);

  const requiredPoints = useCallback(() => {
    if (activeTool === 'elliottWave') {
      const wt = activeSettings().waveType || 'impulse';
      return wt === 'corrective' ? 4 : 6;
    }
    if (activeTool === 'harmonicABCD') return 4;
    return 0;
  }, [activeTool, activeSettings]);

  const getSnapPoints = useCallback(() => {
    const pts = [];
    drawings.forEach((d) => {
      if (d.start) pts.push(d.start);
      if (d.end) pts.push(d.end);
      if (d.points) d.points.forEach((p) => pts.push(p));
    });
    return pts;
  }, [drawings]);

  const snapToNearby = useCallback((pos) => {
    const mag = magneticSnap(pos, candlePixels);
    if (mag !== pos) return mag;
    const pts = getSnapPoints();
    let closest = null;
    let minDist = SNAP_RADIUS;
    pts.forEach((p) => {
      const dist = Math.sqrt((p.x - pos.x) ** 2 + (p.y - pos.y) ** 2);
      if (dist < minDist) { minDist = dist; closest = p; }
    });
    return closest || pos;
  }, [getSnapPoints, candlePixels]);

  const getSVGPoint = useCallback((e) => {
    const svg = svgRef.current;
    if (!svg) return { x: 0, y: 0 };
    const rect = svg.getBoundingClientRect();
    return { x: e.clientX - rect.left, y: e.clientY - rect.top };
  }, []);

  // Multi-click handler (Elliott Wave, Harmonic ABCD)
  const handleMultiClick = useCallback((e) => {
    const pos = snapToNearby(getSVGPoint(e));
    const needed = requiredPoints();
    const next = [...multiPoints, pos];
    if (next.length >= needed) {
      onAddDrawing({ id: Date.now(), tool: activeTool, points: next, settings: activeSettings(), start: next[0], end: next[next.length - 1] });
      setMultiPoints([]);
    } else {
      setMultiPoints(next);
    }
  }, [multiPoints, requiredPoints, activeTool, getSVGPoint, snapToNearby, onAddDrawing, activeSettings]);

  const handleMouseDown = useCallback((e) => {
    if (activeTool === 'cursor') return;
    if (activeTool === 'text')   { setTextInput(getSVGPoint(e)); return; }
    if (isMultiClick)            { handleMultiClick(e); return; }
    const snapped = snapToNearby(getSVGPoint(e));
    setStartPoint(snapped);
    setCurrentPoint(snapped);
    setIsDrawing(true);
  }, [activeTool, getSVGPoint, isMultiClick, handleMultiClick, snapToNearby]);

  const handleMouseMove = useCallback((e) => {
    const pos = snapToNearby(getSVGPoint(e));
    if (isMultiClick && multiPoints.length > 0) { setCurrentPoint(pos); return; }
    if (!isDrawing) return;
    setCurrentPoint(pos);
  }, [isDrawing, isMultiClick, multiPoints.length, getSVGPoint, snapToNearby]);

  const handleMouseUp = useCallback(() => {
    if (!isDrawing || !startPoint || !currentPoint) return;
    onAddDrawing({ id: Date.now(), tool: activeTool, start: startPoint, end: currentPoint, settings: activeSettings() });
    setIsDrawing(false);
    setStartPoint(null);
    setCurrentPoint(null);
  }, [isDrawing, startPoint, currentPoint, activeTool, onAddDrawing, activeSettings]);

  const handleTextSubmit = useCallback((text) => {
    if (!textInput || !text) { setTextInput(null); return; }
    onAddDrawing({ id: Date.now(), tool: 'text', start: textInput, end: textInput, text, settings: activeSettings() });
    setTextInput(null);
  }, [textInput, onAddDrawing, activeSettings]);

  const rulerDistance = (a, b) => Math.sqrt((b.x-a.x)**2 + (b.y-a.y)**2).toFixed(1);
  const rulerAngle    = (a, b) => ((Math.atan2(-(b.y-a.y), b.x-a.x) * 180) / Math.PI).toFixed(1);

  const renderDrawing = (d, isPreview = false) => {
    const opacity = isPreview ? 0.6 : 1;
    const key = isPreview ? 'preview' : d.id;
    const s   = d.settings || {};
    const lw  = s.lineWidth || 2;
    const da  = dashToDashArray(s.dashArray);

    switch (d.tool) {
      case 'elliottWave':
        return renderElliottWave({ ...d, id: key }, isPreview);

      case 'harmonicABCD':
        return renderHarmonicABCD({ ...d, id: key }, isPreview);

      case 'ruler': {
        const color = s.color || '#facc15';
        return (
          <g key={key} opacity={opacity}>
            <line x1={d.start.x} y1={d.start.y} x2={d.end.x} y2={d.end.y} stroke={color} strokeWidth={lw} strokeDasharray={da || '6 3'} />
            <circle cx={d.start.x} cy={d.start.y} r="4" fill={color} />
            <circle cx={d.end.x} cy={d.end.y} r="4" fill={color} />
            {s.showLabel !== false && (
              <>
                <rect x={(d.start.x+d.end.x)/2-44} y={(d.start.y+d.end.y)/2-24} width="88" height="20" rx="4" fill="rgba(0,0,0,0.75)" />
                <text x={(d.start.x+d.end.x)/2} y={(d.start.y+d.end.y)/2-9} textAnchor="middle" fontSize="11" fill={color}>
                  {rulerDistance(d.start, d.end)}px · {rulerAngle(d.start, d.end)}°
                </text>
              </>
            )}
          </g>
        );
      }
      case 'trendline': {
        const color = s.color || '#3b82f6';
        return (
          <g key={key} opacity={opacity}>
            <line x1={d.start.x} y1={d.start.y} x2={d.end.x} y2={d.end.y} stroke={color} strokeWidth={lw} strokeDasharray={da} />
            <circle cx={d.start.x} cy={d.start.y} r="4" fill={color} />
            <circle cx={d.end.x} cy={d.end.y} r="4" fill={color} />
          </g>
        );
      }
      case 'horizontal': {
        const color = s.color || '#22c55e';
        return (
          <g key={key} opacity={opacity}>
            <line x1={0} y1={d.start.y} x2="100%" y2={d.start.y} stroke={color} strokeWidth={lw} strokeDasharray={da || '8 4'} />
            {s.showLabel !== false && (
              <>
                <rect x={d.start.x-20} y={d.start.y-18} width="40" height="16" rx="3" fill={`${color}30`} />
                <text x={d.start.x} y={d.start.y-6} textAnchor="middle" fontSize="10" fill={color}>{Math.round(d.start.y)}</text>
              </>
            )}
          </g>
        );
      }
      case 'rectangle': {
        const color = s.color || '#8b5cf6';
        const hexToRgb = (h) => { const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(h); return m ? [parseInt(m[1],16),parseInt(m[2],16),parseInt(m[3],16)] : [139,92,246]; };
        const [r,g,b] = hexToRgb(color);
        return (
          <g key={key} opacity={opacity}>
            <rect
              x={Math.min(d.start.x, d.end.x)} y={Math.min(d.start.y, d.end.y)}
              width={Math.abs(d.end.x - d.start.x)} height={Math.abs(d.end.y - d.start.y)}
              fill={`rgba(${r},${g},${b},${s.fillOpacity ?? 0.1})`} stroke={color} strokeWidth={lw} strokeDasharray={da}
            />
          </g>
        );
      }
      case 'fibRetracement': {
        const color       = s.color || '#facc15';
        const levels      = (s.levels && s.levels.length > 0) ? s.levels : [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1];
        const levelColors = ['#ef4444','#f97316','#eab308','#22c55e','#3b82f6','#8b5cf6','#ef4444'];
        const top  = Math.min(d.start.y, d.end.y);
        const h    = Math.abs(d.end.y - d.start.y);
        const left = Math.min(d.start.x, d.end.x);
        const w    = Math.abs(d.end.x - d.start.x);
        return (
          <g key={key} opacity={opacity}>
            {levels.map((level, i) => {
              const y  = top + h * level;
              const lc = levelColors[i % levelColors.length];
              return (
                <g key={level}>
                  <line x1={left} y1={y} x2={left+w} y2={y} stroke={lc} strokeWidth={lw} strokeDasharray="4 2" />
                  {s.showLabel !== false && (
                    <text x={left+w+4} y={y+4} fontSize="10" fill={lc}>{(level*100).toFixed(1)}%</text>
                  )}
                </g>
              );
            })}
          </g>
        );
      }
      case 'text':
        return (
          <g key={key} opacity={opacity}>
            <rect x={d.start.x-2} y={d.start.y-14} width={(d.text ? d.text.length*7+8 : 40)} height="20" rx="3" fill="rgba(0,0,0,0.6)" />
            <text x={d.start.x+2} y={d.start.y} fontSize="12" fill="#e5e7eb">{d.text || ''}</text>
          </g>
        );
      default:
        return null;
    }
  };

  // In-progress multi-click preview
  const renderMultiPreview = () => {
    if (!isMultiClick || multiPoints.length === 0) return null;
    const pts = currentPoint ? [...multiPoints, currentPoint] : multiPoints;
    if (pts.length < 2) return null;
    const s = activeSettings();
    return (
      <g opacity="0.55">
        {pts.slice(0,-1).map((pt, i) => (
          <line key={i} x1={pt.x} y1={pt.y} x2={pts[i+1].x} y2={pts[i+1].y}
            stroke={s.color || '#f97316'} strokeWidth={s.lineWidth || 2} />
        ))}
        {pts.map((pt, i) => <circle key={i} cx={pt.x} cy={pt.y} r="4" fill={s.color || '#f97316'} />)}
      </g>
    );
  };

  const isInteractive = activeTool !== 'cursor';
  const cursor = !isInteractive ? 'default' : isMultiClick ? 'cell' : 'crosshair';

  return (
    <div className="absolute inset-0 z-10" style={{ pointerEvents: isInteractive ? 'auto' : 'none' }}>
      <svg
        ref={svgRef}
        className="w-full h-full"
        style={{ cursor }}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={() => {
          if (isDrawing) { setIsDrawing(false); setStartPoint(null); setCurrentPoint(null); }
        }}
      >
        {/* Magnetic snap indicators near cursor */}
        {isInteractive && candlePixels && currentPoint && candlePixels.map((cp, ci) =>
          [cp.openY, cp.highY, cp.lowY, cp.closeY].map((y, yi) => {
            const dist = Math.sqrt((cp.x - currentPoint.x)**2 + (y - currentPoint.y)**2);
            if (dist > MAG_RADIUS * 2) return null;
            return <circle key={`mag-${ci}-${yi}`} cx={cp.x} cy={y} r="5" fill="none" stroke="rgba(251,191,36,0.7)" strokeWidth="1.5" />;
          })
        )}
        {/* Snap-to-endpoint rings */}
        {isInteractive && (isDrawing || (isMultiClick && multiPoints.length > 0)) && getSnapPoints().map((p, i) => (
          <circle key={`snap-${i}`} cx={p.x} cy={p.y} r={SNAP_RADIUS}
            fill="none" stroke="rgba(59,130,246,0.3)" strokeWidth="1" strokeDasharray="3 2" />
        ))}
        {/* Committed drawings */}
        {drawings.map((d) => renderDrawing(d))}
        {/* Drag preview */}
        {isDrawing && startPoint && currentPoint && renderDrawing(
          { id: 'preview', tool: activeTool, start: startPoint, end: currentPoint, settings: activeSettings() }, true
        )}
        {/* Multi-click live preview */}
        {renderMultiPreview()}
      </svg>

      {/* ESC listener while in multi-click mode */}
      {isMultiClick && multiPoints.length > 0 && (
        <EscapeListener onEscape={() => setMultiPoints([])} />
      )}

      {textInput && (
        <TextInputPopup position={textInput} onSubmit={handleTextSubmit} onCancel={() => setTextInput(null)} />
      )}

      {/* Step hint */}
      {isMultiClick && multiPoints.length > 0 && (
        <div className="absolute bottom-2 left-1/2 -translate-x-1/2 bg-gray-900 border border-gray-600 text-xs text-gray-300 px-3 py-1 rounded-full pointer-events-none z-20 whitespace-nowrap">
          {activeTool === 'elliottWave'
            ? `Điểm ${multiPoints.length + 1} / ${requiredPoints()} — ESC để hủy`
            : `Điểm ${['A','B','C','D'][multiPoints.length] || multiPoints.length + 1} — ESC để hủy`}
        </div>
      )}
    </div>
  );
};

// ── Sub-components ────────────────────────────────────────────────
const EscapeListener = ({ onEscape }) => {
  useEffect(() => {
    const h = (e) => { if (e.key === 'Escape') onEscape(); };
    document.addEventListener('keydown', h);
    return () => document.removeEventListener('keydown', h);
  }, [onEscape]);
  return null;
};

const TextInputPopup = ({ position, onSubmit, onCancel }) => {
  const [value, setValue] = useState('');
  const inputRef = useRef(null);
  useEffect(() => { inputRef.current?.focus(); }, []);
  return (
    <div className="absolute z-50 flex items-center gap-1" style={{ left: position.x, top: position.y }}>
      <input
        ref={inputRef}
        type="text"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => { if (e.key === 'Enter') onSubmit(value); if (e.key === 'Escape') onCancel(); }}
        className="bg-gray-700 text-white text-sm rounded px-2 py-1 w-40 border border-gray-500 focus:outline-none focus:border-blue-500"
        placeholder="Nhập ghi chú..."
      />
      <button onClick={() => onSubmit(value)} className="bg-blue-600 text-white text-xs px-2 py-1 rounded hover:bg-blue-700">OK</button>
    </div>
  );
};

export default ChartOverlay;
