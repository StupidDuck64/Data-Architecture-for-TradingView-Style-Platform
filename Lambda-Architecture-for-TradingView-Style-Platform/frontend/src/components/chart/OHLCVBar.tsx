import React from "react";
import { THEME } from "./chartConstants";

interface OHLCVData {
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
  timeLabel?: string;
}

interface OHLCVBarProps {
  data: OHLCVData | null;
}

const OHLCVBar: React.FC<OHLCVBarProps> = ({ data }) => {
  if (!data) return null;
  const isUp = data.close >= data.open;
  const clr = isUp ? THEME.upColor : THEME.downColor;
  const f = (v: number | null | undefined) =>
    v != null
      ? v.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })
      : "-";
  return (
    <div className="flex items-center gap-4 text-xs font-mono select-none flex-wrap">
      {data.timeLabel && (
        <span className="text-gray-400">{data.timeLabel}</span>
      )}
      <span>
        O <span style={{ color: clr }}>{f(data.open)}</span>
      </span>
      <span>
        H <span style={{ color: clr }}>{f(data.high)}</span>
      </span>
      <span>
        L <span style={{ color: clr }}>{f(data.low)}</span>
      </span>
      <span>
        C <span style={{ color: clr }}>{f(data.close)}</span>
      </span>
      {data.volume != null && (
        <span>
          V{" "}
          <span className="text-gray-400">{data.volume.toLocaleString()}</span>
        </span>
      )}
    </div>
  );
};

export default OHLCVBar;
