import React, { useState, useRef, useEffect } from 'react';
import { Search, ChevronDown, Star } from 'lucide-react';
import { useI18n } from '../i18n';

const SYMBOL_META = {
  BTCUSDT:    { icon: '₿', category: 'crypto', name: 'Bitcoin' },
  ETHUSDT:    { icon: 'Ξ', category: 'crypto', name: 'Ethereum' },
  BNBUSDT:    { icon: '◆', category: 'crypto', name: 'BNB' },
  SOLUSDT:    { icon: '◎', category: 'crypto', name: 'Solana' },
  XRPUSDT:    { icon: '✕', category: 'crypto', name: 'XRP' },
  DOGEUSDT:   { icon: '🐕', category: 'crypto', name: 'Dogecoin' },
  ADAUSDT:    { icon: '◈', category: 'crypto', name: 'Cardano' },
  AVAXUSDT:   { icon: '▲', category: 'crypto', name: 'Avalanche' },
  DOTUSDT:    { icon: '●', category: 'crypto', name: 'Polkadot' },
  LINKUSDT:   { icon: '⬡', category: 'crypto', name: 'Chainlink' },
  MATICUSDT:  { icon: '⬟', category: 'crypto', name: 'Polygon' },
  LTCUSDT:    { icon: 'Ł', category: 'crypto', name: 'Litecoin' },
};

const CATEGORIES = ['all', 'crypto', 'indices', 'commodities', 'forex'];

const MarketSelector = ({ symbols, selectedSymbol, onSelect, starredSymbols, onToggleStar }) => {
  const { t } = useI18n();
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [activeCategory, setActiveCategory] = useState('all');
  const dropdownRef = useRef(null);

  useEffect(() => {
    const handleClick = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  const filtered = symbols.filter((s) => {
    const meta = SYMBOL_META[s] || {};
    const matchesSearch = !searchQuery ||
      s.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (meta.name && meta.name.toLowerCase().includes(searchQuery.toLowerCase()));
    const matchesCategory = activeCategory === 'all' || meta.category === activeCategory;
    return matchesSearch && matchesCategory;
  });

  const getCategoryLabel = (cat) => {
    if (cat === 'all') return t('all');
    return t(cat) || cat;
  };

  const meta = SYMBOL_META[selectedSymbol] || {};

  return (
    <div ref={dropdownRef} className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2 bg-gray-700 text-white text-sm rounded px-3 py-1.5 border border-gray-600 hover:border-gray-500 focus:outline-none focus:border-blue-500 cursor-pointer min-w-[160px]"
      >
        {meta.icon && <span className="text-base">{meta.icon}</span>}
        <span className="font-medium">{selectedSymbol}</span>
        <ChevronDown size={14} className="ml-auto text-gray-400" />
      </button>

      {isOpen && (
        <div className="absolute top-full left-0 mt-1 w-72 bg-gray-800 border border-gray-700 rounded-lg shadow-2xl z-[150] overflow-hidden">
          {/* Search */}
          <div className="p-2 border-b border-gray-700">
            <div className="relative">
              <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder={t('searchSymbol')}
                className="w-full bg-gray-700 text-white text-sm rounded px-3 py-1.5 pl-8 border border-gray-600 focus:outline-none focus:border-blue-500"
                autoFocus
              />
            </div>
          </div>

          {/* Category tabs */}
          <div className="flex gap-1 px-2 py-1.5 border-b border-gray-700 overflow-x-auto">
            {CATEGORIES.map((cat) => (
              <button
                key={cat}
                onClick={() => setActiveCategory(cat)}
                className={`px-2 py-0.5 rounded text-xs font-medium whitespace-nowrap transition-colors ${
                  activeCategory === cat
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-400 hover:text-white hover:bg-gray-700'
                }`}
              >
                {getCategoryLabel(cat)}
              </button>
            ))}
          </div>

          {/* Symbol list */}
          <div className="max-h-64 overflow-y-auto">
            {filtered.length === 0 && (
              <div className="px-3 py-4 text-center text-gray-500 text-sm">No results</div>
            )}
            {filtered.map((s) => {
              const m = SYMBOL_META[s] || {};
              const isActive = s === selectedSymbol;
              const isStarred = starredSymbols.includes(s);
              return (
                <div
                  key={s}
                  className={`flex items-center gap-2 px-3 py-2 cursor-pointer transition-colors ${
                    isActive ? 'bg-blue-600 bg-opacity-30' : 'hover:bg-gray-700'
                  }`}
                  onClick={() => { onSelect(s); setIsOpen(false); setSearchQuery(''); }}
                >
                  <span className="text-base w-6 text-center flex-shrink-0">{m.icon || '•'}</span>
                  <div className="flex-1 min-w-0">
                    <span className="text-sm font-medium text-white">{s}</span>
                    {m.name && <span className="text-xs text-gray-400 ml-2">{m.name}</span>}
                  </div>
                  <button
                    onClick={(e) => { e.stopPropagation(); onToggleStar(s); }}
                    className={`p-0.5 rounded transition-colors ${isStarred ? 'text-yellow-400' : 'text-gray-600 hover:text-gray-400'}`}
                  >
                    <Star size={14} fill={isStarred ? 'currentColor' : 'none'} />
                  </button>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export { SYMBOL_META };
export default MarketSelector;
