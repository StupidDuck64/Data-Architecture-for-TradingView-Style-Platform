const translations = {
  en: {
    // Header
    products: "Products",
    community: "Community",
    markets: "Markets",
    news: "News",
    brokers: "Brokers",
    search: "Search",
    login: "Login",
    register: "Register",
    logout: "Logout",
    welcome: "Welcome",

    // Watchlist
    watchlist: "Watchlist",
    starred: "Starred",
    all: "All",

    // Chart tabs
    overview: "Overview",
    candlestick: "Candlestick",
    chart: "Chart",
    orderBook: "Order Book",
    recentTrades: "Recent Trades",

    // Indicators
    indicators: "Indicators",
    technicalIndicators: "Technical Indicators",
    period: "Period",
    color: "Color",
    thickness: "Thickness",
    overbought: "Overbought",
    oversold: "Oversold",

    // Drawing tools
    basic: "Basic",
    patterns: "Patterns",
    delete: "Delete",
    cursor: "Cursor",
    ruler: "Ruler",
    trendline: "Trendline",
    horizontalLine: "Horizontal Line",
    rectangle: "Rectangle",
    fibonacci: "Fibonacci Retracement",
    textNotes: "Text / Notes",
    elliottWave: "Elliott Wave",
    harmonicABCD: "Harmonic ABCD",
    clearAll: "Clear All",

    // Tool settings
    lineWidth: "Line Width",
    lineColor: "Line Color",
    lineWidthPx: "Width (px)",
    showLabel: "Show Label",
    dashStyle: "Dash Style",
    solid: "Solid",
    dashed: "Dashed",
    dotted: "Dotted",
    fillOpacity: "Fill Opacity",
    waveType: "Wave Type",
    impulse: "Impulse",
    corrective: "Corrective",
    fiboRatio: "Fibonacci Ratio:",
    fiboLevels: "Fibonacci Levels:",
    settings: "Settings",

    // Export
    exportChart: "Export Chart",
    exportAsPNG: "Export as PNG",

    // Market selector
    searchSymbol: "Search symbol...",
    crypto: "Crypto",
    indices: "Indices",
    commodities: "Commodities",
    forex: "Forex",
    noResults: "No results",

    // Auth
    email: "Email",
    password: "Password",
    confirmPassword: "Confirm Password",
    loginTitle: "Sign In",
    registerTitle: "Create Account",
    noAccount: "Don't have an account?",
    hasAccount: "Already have an account?",
    signUp: "Sign Up",
    signIn: "Sign In",
    name: "Name",
    passwordsMismatch: "Passwords do not match",
    passwordMinLength: "Password must be at least 6 characters",
    invalidCredentials: "Invalid email or password",
    emailExists: "Email already exists",

    // Order book
    price: "Price",
    amount: "Amount",
    total: "Total",
    bids: "Bids",
    asks: "Asks",
    spread: "Spread",

    // Recent trades
    time: "Time",
    side: "Side",
    buy: "Buy",
    sell: "Sell",

    // Overview
    high24h: "24h High",
    low24h: "24h Low",
    volume24h: "24h Volume",
    change24h: "24h Change",
    open: "Open",
    high: "High",
    low: "Low",
    close: "Close",
    volume: "Volume",
    marketCap: "Market Cap",
    noData: "No data",

    // Language
    language: "Language",
    english: "English",
    vietnamese: "Vietnamese",

    // Points (drawing)
    point: "Point",
    of: "of",
    pointProgress: "Point {n} / {total} — ESC to cancel",
    toCancel: "to cancel",
    enterNote: "Enter note...",

    // Error handling & status
    loading: "Loading…",
    retry: "Retry",
    connectionError:
      "Unable to connect to server — showing cached/fallback data",
    failedLoadCandles: "Failed to load candle data",
    failedLoadOrderBook: "Failed to load order book",
    failedLoadTrades: "Failed to load trades",
    noDataAvailable: "No data available",
    returnToLive: "Return to Live",
    somethingWentWrong: "Something went wrong",
    unexpectedError: "An unexpected error occurred.",
    tryAgain: "Try again",

    // DateRangePicker
    selectDateRange: "Select Date Range",
    start: "Start",
    end: "End",
    apply: "Apply",
    historicalModeTooltip: "Historical mode — click to change",
    queryHistorical: "Query historical data",
    historical: "Historical",
    history: "History",
    live: "Live",

    // System health
    systemDiagnostics: "System diagnostics",
    status: "Status",
    loadingHealth: "Loading health metrics...",
    apiRtt: "API RTT",
    serverHealth: "Server Health",
    totalCheck: "Total Check",
    uptime: "Uptime",
    dependencies: "Dependencies",
    cache: "Cache",
    timeSeriesDb: "Time-Series DB",
    queryEngine: "Query Engine",
    updated: "Updated:",
    warning: "Warning:",

    // Watchlist empty states
    noStarredSymbols: "No starred symbols",
    noSymbols: "No symbols",
  },
  vi: {
    // Header
    products: "Sản phẩm",
    community: "Cộng đồng",
    markets: "Thị trường",
    news: "Tin tức",
    brokers: "Sàn giao dịch",
    search: "Tìm kiếm",
    login: "Đăng nhập",
    register: "Đăng ký",
    logout: "Đăng xuất",
    welcome: "Xin chào",

    // Watchlist
    watchlist: "Danh sách theo dõi",
    starred: "Yêu thích",
    all: "Tất cả",

    // Chart tabs
    overview: "Tổng quan",
    candlestick: "Nến",
    chart: "Biểu đồ",
    orderBook: "Sổ lệnh",
    recentTrades: "Giao dịch gần đây",

    // Indicators
    indicators: "Chỉ báo",
    technicalIndicators: "Chỉ báo kỹ thuật",
    period: "Chu kỳ",
    color: "Màu",
    thickness: "Độ dày",
    overbought: "Quá mua",
    oversold: "Quá bán",

    // Drawing tools
    basic: "Cơ bản",
    patterns: "Mô hình",
    delete: "Xóa",
    cursor: "Con trỏ",
    ruler: "Thước đo",
    trendline: "Đường xu hướng",
    horizontalLine: "Đường ngang",
    rectangle: "Hình chữ nhật",
    fibonacci: "Fibonacci",
    textNotes: "Ghi chú",
    elliottWave: "Sóng Elliott",
    harmonicABCD: "Harmonic ABCD",
    clearAll: "Xóa tất cả",

    // Tool settings
    lineWidth: "Độ dày nét",
    lineColor: "Màu đường",
    lineWidthPx: "Độ dày (px)",
    showLabel: "Hiện nhãn",
    dashStyle: "Kiểu nét",
    solid: "Liền",
    dashed: "Đứt đoạn",
    dotted: "Chấm",
    fillOpacity: "Độ mờ nền",
    waveType: "Loại sóng",
    impulse: "Xung lực",
    corrective: "Điều chỉnh",
    fiboRatio: "Tỷ lệ Fibonacci:",
    fiboLevels: "Mức Fibonacci:",
    settings: "Cài đặt",

    // Export
    exportChart: "Xuất biểu đồ",
    exportAsPNG: "Xuất dạng PNG",

    // Market selector
    searchSymbol: "Tìm mã...",
    crypto: "Tiền mã hóa",
    indices: "Chỉ số",
    commodities: "Hàng hóa",
    forex: "Ngoại hối",
    noResults: "Không có kết quả",

    // Auth
    email: "Email",
    password: "Mật khẩu",
    confirmPassword: "Xác nhận mật khẩu",
    loginTitle: "Đăng nhập",
    registerTitle: "Tạo tài khoản",
    noAccount: "Chưa có tài khoản?",
    hasAccount: "Đã có tài khoản?",
    signUp: "Đăng ký",
    signIn: "Đăng nhập",
    name: "Tên",
    passwordsMismatch: "Mật khẩu không khớp",
    passwordMinLength: "Mật khẩu phải có ít nhất 6 ký tự",
    invalidCredentials: "Email hoặc mật khẩu không đúng",
    emailExists: "Email đã tồn tại",

    // Order book
    price: "Giá",
    amount: "Khối lượng",
    total: "Tổng",
    bids: "Mua",
    asks: "Bán",
    spread: "Chênh lệch",

    // Recent trades
    time: "Thời gian",
    side: "Loại",
    buy: "Mua",
    sell: "Bán",

    // Overview
    high24h: "Cao 24h",
    low24h: "Thấp 24h",
    volume24h: "KL 24h",
    change24h: "Thay đổi 24h",
    open: "Mở cửa",
    high: "Cao nhất",
    low: "Thấp nhất",
    close: "Đóng cửa",
    volume: "Khối lượng",
    marketCap: "Vốn hóa",
    noData: "Không có dữ liệu",

    // Language
    language: "Ngôn ngữ",
    english: "Tiếng Anh",
    vietnamese: "Tiếng Việt",

    // Points (drawing)
    point: "Điểm",
    of: "/",
    pointProgress: "Điểm {n} / {total} — ESC để hủy",
    toCancel: "để hủy",
    enterNote: "Nhập ghi chú...",

    // Error handling & status
    loading: "Đang tải…",
    retry: "Thử lại",
    connectionError:
      "Không thể kết nối máy chủ — hiển thị dữ liệu tạm",
    failedLoadCandles: "Không tải được dữ liệu nến",
    failedLoadOrderBook: "Không tải được sổ lệnh",
    failedLoadTrades: "Không tải được giao dịch",
    noDataAvailable: "Không có dữ liệu",
    returnToLive: "Quay về trực tiếp",
    somethingWentWrong: "Đã xảy ra lỗi",
    unexpectedError: "Đã xảy ra lỗi không mong muốn.",
    tryAgain: "Thử lại",

    // DateRangePicker
    selectDateRange: "Chọn khoảng thời gian",
    start: "Bắt đầu",
    end: "Kết thúc",
    apply: "Áp dụng",
    historicalModeTooltip: "Chế độ lịch sử — nhấn để thay đổi",
    queryHistorical: "Truy vấn dữ liệu lịch sử",
    historical: "Lịch sử",
    history: "Lịch sử",
    live: "Trực tiếp",

    // System health
    systemDiagnostics: "Chẩn đoán hệ thống",
    status: "Trạng thái",
    loadingHealth: "Đang tải thông tin hệ thống...",
    apiRtt: "Thời gian API",
    serverHealth: "Sức khỏe máy chủ",
    totalCheck: "Tổng kiểm tra",
    uptime: "Thời gian hoạt động",
    dependencies: "Phụ thuộc",
    cache: "Bộ nhớ đệm",
    timeSeriesDb: "CSDL chuỗi thời gian",
    queryEngine: "Trình truy vấn",
    updated: "Cập nhật:",
    warning: "Cảnh báo:",

    // Watchlist empty states
    noStarredSymbols: "Chưa có mã yêu thích",
    noSymbols: "Không có mã nào",
  },
} as const;

export type TranslationKey = keyof (typeof translations)["en"];
export default translations;
