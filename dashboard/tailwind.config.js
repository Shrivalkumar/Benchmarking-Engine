/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "#09090b",
        card: "#18181b",
        border: "#27272a",
        accent: "#3f3f46",
        primary: "#10b981", // Emerald Green for speed/success
        secondary: "#3b82f6", // Blue
        danger: "#ef4444", // Red
      },
      fontFamily: {
        sans: ["Inter", "sans-serif"],
        mono: ["JetBrains Mono", "monospace"],
      },
    },
  },
  plugins: [],
}
