import React, { useState, useRef, useEffect } from 'react';
import { Search, Mic, Filter, X, Sparkles, TrendingUp } from 'lucide-react';

const SearchBar = ({ 
  onSearch, 
  onSearchTypeChange, 
  searchType = 'simple',
  loading = false,
  suggestions = [],
  currentUser,
  contentTypeFilter,
  onContentTypeChange 
}) => {
  const [query, setQuery] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [showFilters, setShowFilters] = useState(false);
  const [isListening, setIsListening] = useState(false);
  const inputRef = useRef(null);
  const recognition = useRef(null);

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
        handleSearch(transcript);
      };

      recognition.current.onerror = () => {
        setIsListening(false);
      };

      recognition.current.onend = () => {
        setIsListening(false);
      };
    }
  }, []);

  const handleSearch = (searchQuery = query) => {
    if (searchQuery.trim()) {
      onSearch(searchQuery.trim());
      setShowSuggestions(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleSearch();
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
    setShowSuggestions(false);
    inputRef.current?.focus();
  };

  const predefinedQueries = [
    "Show me trending videos",
    "Find educational courses for programming",
    "Recommend action movies similar to Marvel",
    "Popular music from this year",
    "Best products for home office",
    "Comedy podcasts",
    "Learn web development",
    "Healthy recipes and cooking"
  ];

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
              onChange={(e) => setQuery(e.target.value)}
              onKeyPress={handleKeyPress}
              onFocus={() => setShowSuggestions(true)}
              placeholder={
                searchType === 'ai' 
                  ? "Ask me anything... 'Show me action movies like Marvel' or 'Find educational programming content'"
                  : "Search for content..."
              }
              className="w-full px-4 py-4 text-lg bg-transparent focus:outline-none placeholder-gray-400"
              disabled={loading}
            />
            
            {query && (
              <button
                onClick={clearSearch}
                className="absolute right-4 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600 transition-colors"
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

      {/* Search Suggestions */}
      {showSuggestions && !loading && (
        <div className="absolute top-full left-0 right-0 mt-2 bg-white rounded-xl shadow-xl border border-gray-200 overflow-hidden z-50">
          <div className="p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-gray-700 flex items-center">
                <TrendingUp size={16} className="mr-2" />
                {searchType === 'ai' ? 'Try asking...' : 'Trending searches'}
              </h3>
              <button
                onClick={() => setShowSuggestions(false)}
                className="text-gray-400 hover:text-gray-600"
              >
                <X size={16} />
              </button>
            </div>
            
            <div className="space-y-1">
              {predefinedQueries.slice(0, 6).map((suggestion, index) => (
                <button
                  key={index}
                  onClick={() => {
                    setQuery(suggestion);
                    handleSearch(suggestion);
                  }}
                  className="w-full text-left px-3 py-2 rounded-lg text-sm text-gray-600 hover:bg-blue-50 hover:text-blue-600 transition-colors duration-200"
                >
                  {suggestion}
                </button>
              ))}
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