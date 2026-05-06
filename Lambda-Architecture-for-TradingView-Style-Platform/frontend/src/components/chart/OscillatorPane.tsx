import React, { useEffect, useRef } from "react";
import { createChart, CrosshairMode, LineStyle, LineSeries, type UTCTimestamp } from "lightweight-charts";
import { THEME, localTickMarkFormatter, localTimeFormatter } from "./chartConstants";
import type { IndicatorSettings } from "../../types";

interface DataPoint {
  time: number;
  value: number;
}

interface OscillatorPaneProps {
  data: DataPoint[];
  settings: IndicatorSettings;
  label: string;
}

const OscillatorPane: React.FC<OscillatorPaneProps> = ({ data, settings, label }) => {
  const ref = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const seriesRef = useRef<ReturnType<ReturnType<typeof createChart>["addSeries"]> | null>(null);
  const prevLenRef = useRef(0);

  useEffect(() => {
    if (!ref.current || !data || data.length === 0) return;
    const chart = createChart(ref.current, {
      layout: { background: { color: "#141620" }, textColor: THEME.textColor, fontSize: 10 },
      grid: { vertLines: { color: THEME.gridColor }, horzLines: { color: THEME.gridColor } },
      localization: { locale: navigator.language || "en-US", timeFormatter: localTimeFormatter },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: THEME.borderColor, scaleMargins: { top: 0.1, bottom: 0.1 } },
      timeScale: { borderColor: THEME.borderColor, timeVisible: true, secondsVisible: false, tickMarkFormatter: localTickMarkFormatter },
      handleScroll: false,
      handleScale: false,
    });
    const series = chart.addSeries(LineSeries, {
      color: settings.color,
      lineWidth: (settings.lineWidth || 1.5) as 1 | 2 | 3 | 4,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerVisible: true,
    });
    series.setData(data.map(d => ({ ...d, time: d.time as UTCTimestamp })));
    prevLenRef.current = data.length;

    if (settings.overbought != null) {
      series.createPriceLine({ price: settings.overbought, color: "#ef444460", lineStyle: LineStyle.Dashed, lineWidth: 1, title: "OB", axisLabelVisible: true });
      series.createPriceLine({ price: settings.oversold ?? 30, color: "#22c55e60", lineStyle: LineStyle.Dashed, lineWidth: 1, title: "OS", axisLabelVisible: true });
    }

    chart.timeScale().fitContent();
    chartRef.current = chart;
    seriesRef.current = series;
    const ro = new ResizeObserver(() => {
      if (ref.current) chart.resize(ref.current.clientWidth, ref.current.clientHeight);
    });
    ro.observe(ref.current);
    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!seriesRef.current || !data || data.length === 0) return;
    if (data.length !== prevLenRef.current) {
      seriesRef.current.setData(data.map(d => ({ ...d, time: d.time as UTCTimestamp })));
      prevLenRef.current = data.length;
    } else {
      seriesRef.current.update({ ...data[data.length - 1], time: data[data.length - 1].time as UTCTimestamp });
    }
  }, [data]);

  return (
    <div className="relative border-t border-gray-700" style={{ height: 100 }}>
      <span className="absolute top-1 left-2 text-xs text-gray-400 z-10 pointer-events-none">{label}</span>
      <div ref={ref} className="w-full h-full" />
    </div>
  );
};

export default OscillatorPane;
