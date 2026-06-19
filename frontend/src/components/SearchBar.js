import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Search, Mic, Filter, X, Sparkles, TrendingUp, Clock } from 'lucide-react';
import axios from 'axios';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://127.0.0.1:8000';
const API = `${BACKEND_URL}/api`;

const CONTENT_TYPE_ICONS = {
  video: '🎥', movie: '🍿', article: '📰', product: '🛍️',
  music: '🎵', podcast: '🎧', course: '📚', game: '🎮'
};

const SearchBar = ({ 
  onSearch, 
  onSearchTypeChange, 
  searchType = 'simple',
  loading = false,
  currentUser,
  contentTypeFilter,
  onContentTypeChange 
}) => {
  const [query, setQuery] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [showFilters, setShowFilters] = useState(false);
  const [isListening, setIsListening] = useState(false);
  // Phase 1: live autocomplete state
  const [liveSuggestions, setLiveSuggestions] = useState([]);
  const [activeSuggestionIdx, setActiveSuggestionIdx] = useState(-1);
  const [recentSearches, setRecentSearches] = useState(() => {
    try { return JSON.parse(localStorage.getItem('recentSearches') || '[]'); }
    catch { return []; }
  });
  const inputRef = useRef(null);
  const recognition = useRef(null);
  const debounceTimer = useRef(null);
  // BUG-8 fix: keep a ref to latest handleSearch so voice search never uses a stale closure
  const handleSearchRef = useRef(null);

  // Initialize speech recognition
  useEffect(() => {
    if ('webkitSpeechRecognition' in window) {
      recognition.current = new window.webkitSpeechRecognition();
      recognition.current.continuous = false;
      recognition.current.interimResults = false;
      recognition.current.lang = 'en-US';

      recognition.current.onresult = (event) => {
        const transcript = event.results[0][0].transcript;
        setQuery(transcript);
        setIsListening(false);
        // BUG-8 fix: call via ref so we always get the latest handleSearch
        handleSearchRef.current(transcript);
      };

      recognition.current.onerror = () => {
        setIsListening(false);
      };

      recognition.current.onend = () => {
        setIsListening(false);
      };
    }
  }, []);

  // Phase 1: fetch live autocomplete suggestions (debounced 300ms)
  const fetchSuggestions = useCallback(async (q) => {
    if (!q || q.length < 2) { setLiveSuggestions([]); return; }
    try {
      const params = new URLSearchParams({ q, limit: 6 });
      if (contentTypeFilter) params.append('content_type', contentTypeFilter);
      const res = await axios.get(`${API}/search/suggest?${params}`);
      setLiveSuggestions(res.data.suggestions || []);
    } catch {
      setLiveSuggestions([]);
    }
  }, [contentTypeFilter]);

  const handleSearch = (searchQuery = query) => {
    if (searchQuery.trim()) {
      onSearch(searchQuery.trim());
      setShowSuggestions(false);
      setActiveSuggestionIdx(-1);
      // Phase 1: save to recent searches (keep last 5)
      const updated = [searchQuery.trim(), ...recentSearches.filter(s => s !== searchQuery.trim())].slice(0, 5);
      setRecentSearches(updated);
      try { localStorage.setItem('recentSearches', JSON.stringify(updated)); } catch {}
    }
  };
  // BUG-8 fix: always update the ref so voice-search callback always calls latest version
  handleSearchRef.current = handleSearch;

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      if (activeSuggestionIdx >= 0 && allSuggestions[activeSuggestionIdx]) {
        const sel = allSuggestions[activeSuggestionIdx];
        const title = typeof sel === 'string' ? sel : sel.title;
        setQuery(title);
        handleSearch(title);
      } else {
        handleSearch();
      }
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActiveSuggestionIdx(i => Math.min(i + 1, allSuggestions.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActiveSuggestionIdx(i => Math.max(i - 1, -1));
    } else if (e.key === 'Escape') {
      setShowSuggestions(false);
      setActiveSuggestionIdx(-1);
    }
  };

  const handleVoiceSearch = () => {
    if (recognition.current && !isListening) {
      setIsListening(true);
      recognition.current.start();
    }
  };

  const clearSearch = () => {
    setQuery('');
    setLiveSuggestions([]);
    setShowSuggestions(false);
    setActiveSuggestionIdx(-1);
    inputRef.current?.focus();
  };

  // Merge live suggestions + fallback predefined queries
  const predefinedQueries = [
    "Show me trending videos",
    "Find educational courses for programming",
    "Recommend action movies similar to Marvel",
    "Popular music from this year",
    "Best products for home office",
    "Comedy podcasts",
  ];

  // allSuggestions: live API results first, then recent searches, then predefined
  const allSuggestions = [
    ...liveSuggestions,
    ...recentSearches
      .filter(r => !liveSuggestions.some(s => (typeof s === 'string' ? s : s.title) === r))
      .map(r => ({ title: r, content_type: '', category: '', isRecent: true })),
    ...(liveSuggestions.length === 0
      ? predefinedQueries.map(q => ({ title: q, content_type: '', category: '', isPredefined: true }))
      : []),
  ].slice(0, 8);

  const contentTypes = [
    { value: '', label: 'All Content', icon: '🌐' },
    { value: 'video', label: 'Videos', icon: '🎥' },
    { value: 'movie', label: 'Movies', icon: '🍿' },
    { value: 'article', label: 'Articles', icon: '📰' },
    { value: 'product', label: 'Products', icon: '🛍️' },
    { value: 'music', label: 'Music', icon: '🎵' },
    { value: 'podcast', label: 'Podcasts', icon: '🎧' },
    { value: 'course', label: 'Courses', icon: '📚' },
    { value: 'game', label: 'Games', icon: '🎮' }
  ];

  return (
    <div className="relative w-full max-w-4xl mx-auto">
      {/* Main Search Bar */}
      <div className="relative bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
        <div className="flex items-center">
          {/* Search Type Toggle */}
          <div className="flex border-r border-gray-200">
            <button
              onClick={() => onSearchTypeChange('simple')}
              className={`px-4 py-4 flex items-center space-x-2 transition-all duration-200 ${
                searchType === 'simple' 
                  ? 'bg-blue-50 text-blue-600 border-b-2 border-blue-600' 
                  : 'text-gray-500 hover:text-gray-700 hover:bg-gray-50'
              }`}
              title="Simple text search"
            >
              <Search size={20} />
              <span className="hidden sm:inline text-sm font-medium">Simple</span>
            </button>
            
            <button
              onClick={() => onSearchTypeChange('ai')}
              className={`px-4 py-4 flex items-center space-x-2 transition-all duration-200 ${
                searchType === 'ai' 
                  ? 'bg-gradient-to-r from-purple-50 to-pink-50 text-purple-600 border-b-2 border-purple-600' 
                  : 'text-gray-500 hover:text-gray-700 hover:bg-gray-50'
              }`}
              title="AI-powered natural language search"
            >
              <Sparkles size={20} />
              <span className="hidden sm:inline text-sm font-medium">AI</span>
            </button>
          </div>

          {/* Search Input */}
          <div className="flex-1 relative">
            <input
              ref={inputRef}
              type="text"
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
                setActiveSuggestionIdx(-1);
                // Phase 1: debounced autocomplete
                clearTimeout(debounceTimer.current);
                debounceTimer.current = setTimeout(() => fetchSuggestions(e.target.value), 300);
              }}
              onKeyDown={handleKeyPress}
              onFocus={() => setShowSuggestions(true)}
              placeholder={
                searchType === 'ai' 
                  ? "Ask me anything... 'Show me action movies like Marvel'"
                  : "Search for content..."
              }
              className="w-full px-4 py-4 text-lg bg-transparent focus:outline-none placeholder-gray-400"
              disabled={loading}
              autoComplete="off"
              aria-autocomplete="list"
              aria-expanded={showSuggestions && allSuggestions.length > 0}
            />
            
            {query && (
              <button
                onClick={clearSearch}
                className="absolute right-4 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600 transition-colors"
                aria-label="Clear search"
              >
                <X size={20} />
              </button>
            )}
          </div>

          {/* Action Buttons */}
          <div className="flex items-center border-l border-gray-200">
            {/* Voice Search */}
            {'webkitSpeechRecognition' in window && (
              <button
                onClick={handleVoiceSearch}
                disabled={isListening || loading}
                className={`p-4 transition-all duration-200 ${
                  isListening 
                    ? 'text-red-500 animate-pulse' 
                    : 'text-gray-500 hover:text-blue-600 hover:bg-blue-50'
                }`}
                title="Voice search"
              >
                <Mic size={20} />
              </button>
            )}

            {/* Filter Toggle */}
            <button
              onClick={() => setShowFilters(!showFilters)}
              className={`p-4 transition-all duration-200 ${
                showFilters 
                  ? 'text-blue-600 bg-blue-50' 
                  : 'text-gray-500 hover:text-blue-600 hover:bg-blue-50'
              }`}
              title="Filters"
            >
              <Filter size={20} />
            </button>

            {/* Search Button */}
            <button
              onClick={() => handleSearch()}
              disabled={!query.trim() || loading}
              className="px-6 py-4 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 disabled:from-gray-300 disabled:to-gray-400 text-white font-medium transition-all duration-200 transform hover:scale-105 disabled:transform-none disabled:cursor-not-allowed"
            >
              {loading ? (
                <div className="animate-spin w-5 h-5 border-2 border-white border-t-transparent rounded-full" />
              ) : (
                <Search size={20} />
              )}
            </button>
          </div>
        </div>

        {/* Filters Panel */}
        {showFilters && (
          <div className="border-t border-gray-200 bg-gray-50 p-4">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium text-gray-700 mr-2">Content Type:</span>
              {contentTypes.map((type) => (
                <button
                  key={type.value}
                  onClick={() => onContentTypeChange(type.value)}
                  className={`flex items-center space-x-1 px-3 py-1.5 rounded-full text-sm font-medium transition-all duration-200 ${
                    contentTypeFilter === type.value
                      ? 'bg-blue-600 text-white shadow-md'
                      : 'bg-white text-gray-600 hover:bg-blue-50 hover:text-blue-600 border border-gray-200'
                  }`}
                >
                  <span>{type.icon}</span>
                  <span>{type.label}</span>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Search Suggestions / Autocomplete Dropdown */}
      {showSuggestions && !loading && allSuggestions.length > 0 && (
        <div className="autocomplete-dropdown absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-xl border border-gray-200 overflow-hidden z-50">
          <div className="p-3">
            <div className="flex items-center justify-between mb-2 px-1">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide flex items-center gap-1.5">
                <TrendingUp size={12} />
                {liveSuggestions.length > 0 ? 'Suggestions' : recentSearches.length > 0 ? 'Recent searches' : 'Try asking...'}
              </h3>
              <button onClick={() => setShowSuggestions(false)} className="text-gray-400 hover:text-gray-600" aria-label="Close suggestions">
                <X size={14} />
              </button>
            </div>

            <div className="space-y-0.5" role="listbox">
              {allSuggestions.map((sugg, index) => {
                const title = typeof sugg === 'string' ? sugg : sugg.title;
                const ctype = typeof sugg === 'string' ? '' : sugg.content_type;
                const isRecent = typeof sugg !== 'string' && sugg.isRecent;
                const isActive = index === activeSuggestionIdx;

                return (
                  <button
                    key={index}
                    role="option"
                    aria-selected={isActive}
                    onClick={() => {
                      setQuery(title);
                      handleSearch(title);
                    }}
                    className={`w-full text-left px-3 py-2.5 rounded-lg text-sm transition-colors duration-150 flex items-center gap-3 ${
                      isActive ? 'bg-blue-50 text-blue-700' : 'text-gray-700 hover:bg-gray-50'
                    }`}
                  >
                    <span className="text-base flex-shrink-0">
                      {isRecent ? <Clock size={14} className="text-gray-400" /> : (CONTENT_TYPE_ICONS[ctype] || '🔍')}
                    </span>
                    <span className="flex-1 truncate">{title}</span>
                    {ctype && !isRecent && (
                      <span className="text-xs text-gray-400 flex-shrink-0 capitalize">{ctype}</span>
                    )}
                    {isRecent && <span className="text-xs text-gray-400 flex-shrink-0">Recent</span>}
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* Search Type Info */}
      <div className="mt-3 flex items-center justify-center space-x-4 text-xs text-gray-500">
        {searchType === 'ai' ? (
          <div className="flex items-center space-x-1">
            <Sparkles size={14} className="text-purple-500" />
            <span>AI-powered search understands natural language queries</span>
          </div>
        ) : (
          <div className="flex items-center space-x-1">
            <Search size={14} className="text-blue-500" />
            <span>Search by keywords, titles, categories, or tags</span>
          </div>
        )}
      </div>
    </div>
  );
};

export default SearchBar;