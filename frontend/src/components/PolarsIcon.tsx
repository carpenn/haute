import { memo } from "react"

interface PolarsIconProps {
  size?: number
  color?: string
  className?: string
  style?: React.CSSProperties
}

function PolarsIcon({ size = 16, color = "currentColor", className, style }: PolarsIconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      style={style}
    >
      {/* Simplified Polars logo: vertical bars of varying heights */}
      <rect x="1" y="8" width="2" height="6" rx="0.5" fill={color} opacity="0.7" />
      <rect x="4" y="4" width="2" height="10" rx="0.5" fill={color} opacity="0.85" />
      <rect x="7" y="2" width="2" height="12" rx="0.5" fill={color} />
      <rect x="10" y="5" width="2" height="9" rx="0.5" fill={color} opacity="0.85" />
      <rect x="13" y="7" width="2" height="7" rx="0.5" fill={color} opacity="0.7" />
    </svg>
  )
}

export default memo(PolarsIcon)
