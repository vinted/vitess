module.exports = {
    purge: ['./src/**/*.{js,jsx,ts,tsx}', './public/index.html'],
    darkMode: false,
    theme: {
        extend: {
            textColor: {
                primary: '#17171b',
                secondary: '#718096',
                danger: '#D32F2F',
                warning: '#FFAB40',
                success: '#00893E',
                none: '#17171b'
            },
        },
        fontFamily: {
            mono: ['NotoMono', 'source-code-pro', 'menlo', 'monaco', 'consolas', 'Courier New', 'monospace'],
            sans: [
                'NotoSans',
                '-apple-system',
                'blinkmacsystemfont',
                'Segoe UI',
                'Roboto',
                'Oxygen',
                'Ubuntu',
                'Cantarell',
                'Fira Sans',
                'Droid Sans',
                'Helvetica Neue',
                'sans-serif',
            ],
        },
    },
    variants: {
        extend: {},
    },
    plugins: [],
};
