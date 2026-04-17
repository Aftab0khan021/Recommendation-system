import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import './App.css';

// Import our enhanced components
import Navigation from './components/Navigation';
import SearchBar from './components/SearchBar';
import StatsDashboard from './components/StatsDashboard';
import RecommendationCard from './components/RecommendationCard';
import ItemModal from './components/ItemModal';
import LoadingSpinner from './components/LoadingSpinner';

// Icons
import { AlertCircle, RefreshCw, Search, Sparkles, TrendingUp, Filter } from 'lucide-react';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://127.0.0.1:8000';
const API = `${BACKEND_URL}/api`;

const App = () => {
  // State management
  const [currentUser, setCurrentUser] = useState('demo_user_1');
  const [recommendations, setRecommendations] = useState([]);
  const [searchResults, setSearchResults] = useState([]);
  const [contentTypeFilter, setContentTypeFilter] = useState('');
  const [searchType, setSearchType] = useState('simple');
  const [loading, setLoading] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [stats, setStats] = useState(null);
  const [abTestInfo, setAbTestInfo] = useState(null);
  const [algorithm, setAlgorithm] = useState('');
  const [error, setError] = useState(null);
  const [selectedItem, setSelectedItem] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [activeTab, setActiveTab] = useState('recommendations'); // 'recommendations' or 'search'
  const [lastSearchQuery, setLastSearchQuery] = useState('');
  // Bug #18 fix: track page offset for real "Load More" pagination
  const [recPage, setRecPage] = useState(1);
  const [searchPage, setSearchPage] = useState(1);
  // NEW-7 fix: track whether more pages exist so Load More hides when exhausted
  const [hasMoreRecs, setHasMoreRecs] = useState(true);
  const [hasMoreSearch, setHasMoreSearch] = useState(true);
  // NEW-4 fix: React-managed notifications instead of raw document.createElement
  const [notifications, setNotifications] = useState([]);
  const PAGE_SIZE = 20;

  // Fetch recommendations — BUG-14 fix: only fetch the page slice needed, not all pages
  const fetchRecommendations = useCallback(async (page = 1, append = false) => {
    if (!currentUser.trim()) return;

    setLoading(true);
    setError(null);
    
    try {
      const params = new URLSearchParams({
        user_id: currentUser,
        n: String(PAGE_SIZE)  // BUG-14 fix: always request only one page worth
      });
      
      if (contentTypeFilter) {
        params.append('content_type', contentTypeFilter);
      }

      const response = await axios.get(`${API}/recommend?${params}`);
      const incoming = response.data.recommendations || [];
      // On fresh load replace list; on load-more append the new page
      setRecommendations(prev => append ? [...prev, ...incoming] : incoming);
      // NEW-7 fix: hide Load More if we got fewer items than requested
      setHasMoreRecs(incoming.length >= PAGE_SIZE);
      setAlgorithm(response.data.algorithm);
    } catch (error) {
      console.error('Error fetching recommendations:', error);
      setError('Failed to fetch recommendations. Please try again.');
      if (!append) setRecommendations([]);
    } finally {
      setLoading(false);
    }
  }, [currentUser, contentTypeFilter, PAGE_SIZE]);

  // Search functionality — BUG-14 fix: request only one page's worth, don't over-fetch
  const handleSearch = async (query, page = 1, append = false) => {
    if (!query.trim()) return;

    setSearchLoading(true);
    setError(null);
    if (!append) {
      setLastSearchQuery(query);
      setActiveTab('search');
      setSearchPage(1);
    }
    
    try {
      const params = new URLSearchParams({
        q: query,
        search_type: searchType,
        limit: String(PAGE_SIZE)  // BUG-14 fix: always fetch exactly one page
      });
      
      if (currentUser) {
        params.append('user_id', currentUser);
      }
      
      if (contentTypeFilter) {
        params.append('content_type', contentTypeFilter);
      }

      const response = await axios.get(`${API}/search?${params}`);
      const incoming = response.data.results || [];
      // On fresh load replace; on load-more append
      setSearchResults(prev => append ? [...prev, ...incoming] : incoming);
      // NEW-7 fix: hide Load More if we got fewer items than requested
      setHasMoreSearch(incoming.length >= PAGE_SIZE);
    } catch (error) {
      console.error('Error searching:', error);
      setError('Search failed. Please try again.');
      if (!append) setSearchResults([]);
    } finally {
      setSearchLoading(false);
    }
  };

  // Fetch system statistics
  const fetchStats = useCallback(async () => {
    try {
      const response = await axios.get(`${API}/stats`);
      setStats(response.data.statistics);
    } catch (error) {
      console.error('Error fetching stats:', error);
    }
  }, []);

  // Fetch A/B test information
  const fetchAbTestInfo = useCallback(async () => {
    if (!currentUser.trim()) return;
    try {
      // NEW-5 fix: use URLSearchParams instead of string interpolation
      const params = new URLSearchParams({ user_id: currentUser });
      const response = await axios.get(`${API}/ab/arm?${params}`);
      setAbTestInfo(response.data);
    } catch (error) {
      console.error('Error fetching A/B test info:', error);
    }
  }, [currentUser]);

  // Log user interaction
  const logInteraction = async (itemId, interactionType, context = {}) => {
    try {
      const eventData = {
        user_id: currentUser,
        item_id: itemId,
        type: interactionType,
        // NEW-3 fix: send client-side timestamp so logged time reflects actual interaction
        ts: new Date().toISOString(),
        dwell_seconds: context.view_duration || 0,
        context: context
      };

      await axios.post(`${API}/event`, eventData);
      console.log(`Logged ${interactionType} interaction for item ${itemId}`);
      showNotification(`${interactionType.charAt(0).toUpperCase() + interactionType.slice(1)} recorded! 🎉`, 'success');
    } catch (error) {
      console.error('Error logging interaction:', error);
      showNotification('Error logging interaction', 'error');
    }
  };

  // NEW-4 fix: React state-based notifications — no raw document.createElement
  const showNotification = useCallback((message, type = 'success') => {
    const id = Date.now();
    setNotifications(prev => [...prev, { id, message, type }]);
    setTimeout(() => {
      setNotifications(prev => prev.filter(n => n.id !== id));
    }, 3000);
  }, []);

  // Handle item click - open modal
  const handleItemClick = (item) => {
    setSelectedItem(item);
    setIsModalOpen(true);
  };

  // Handle refresh — resets to page 1
  const handleRefresh = () => {
    setRecPage(1);
    setSearchPage(1);
    if (activeTab === 'recommendations') {
      fetchRecommendations(1, false);
    } else if (lastSearchQuery) {
      handleSearch(lastSearchQuery, 1, false);
    }
    fetchStats();
    fetchAbTestInfo();
  };

  // Bug #18 fix: real load-more — increments page and appends
  const handleLoadMore = () => {
    if (activeTab === 'recommendations') {
      const nextPage = recPage + 1;
      setRecPage(nextPage);
      fetchRecommendations(nextPage, true);
    } else if (lastSearchQuery) {
      const nextPage = searchPage + 1;
      setSearchPage(nextPage);
      handleSearch(lastSearchQuery, nextPage, true);
    }
  };

  // Load data on component mount and user/filter change (reset pagination)
  useEffect(() => {
    setRecPage(1);
    fetchRecommendations(1, false);
    fetchAbTestInfo();
  }, [fetchRecommendations, fetchAbTestInfo]);

  useEffect(() => {
    fetchStats();
    // Refresh stats every 30 seconds
    const interval = setInterval(fetchStats, 30000);
    return () => clearInterval(interval);
  }, [fetchStats]);

  // Current results based on active tab
  const currentResults = activeTab === 'search' ? searchResults : recommendations;
  const currentLoading = activeTab === 'search' ? searchLoading : loading;

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 via-blue-50 to-purple-50">
      {/* Navigation */}
      <Navigation
        currentUser={currentUser}
        onUserChange={setCurrentUser}
        onRefresh={handleRefresh}
        loading={currentLoading}
        stats={stats}
      />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Search Bar */}
        <div className="mb-8">
          <SearchBar
            onSearch={handleSearch}
            onSearchTypeChange={setSearchType}
            searchType={searchType}
            loading={searchLoading}
            currentUser={currentUser}
            contentTypeFilter={contentTypeFilter}
            onContentTypeChange={setContentTypeFilter}
          />
        </div>

        {/* Stats Dashboard */}
        <StatsDashboard 
          stats={stats} 
          abTestInfo={abTestInfo} 
          className="mb-8"
        />

        {/* Tab Navigation */}
        <div className="flex items-center justify-between mb-8">
          <div className="flex space-x-4">
            <button
              onClick={() => setActiveTab('recommendations')}
              className={`flex items-center space-x-2 px-6 py-3 rounded-xl font-semibold transition-all duration-200 ${
                activeTab === 'recommendations'
                  ? 'bg-blue-600 text-white shadow-lg transform scale-105'
                  : 'bg-white text-gray-600 hover:bg-blue-50 hover:text-blue-600 border border-gray-200'
              }`}
            >
              <TrendingUp size={20} />
              <span>Recommendations</span>
              {recommendations.length > 0 && (
                <span className="bg-blue-500 text-white text-xs px-2 py-1 rounded-full">
                  {recommendations.length}
                </span>
              )}
            </button>
            
            <button
              onClick={() => setActiveTab('search')}
              className={`flex items-center space-x-2 px-6 py-3 rounded-xl font-semibold transition-all duration-200 ${
                activeTab === 'search'
                  ? 'bg-purple-600 text-white shadow-lg transform scale-105'
                  : 'bg-white text-gray-600 hover:bg-purple-50 hover:text-purple-600 border border-gray-200'
              }`}
            >
              {searchType === 'ai' ? <Sparkles size={20} /> : <Search size={20} />}
              <span>Search Results</span>
              {searchResults.length > 0 && (
                <span className="bg-purple-500 text-white text-xs px-2 py-1 rounded-full">
                  {searchResults.length}
                </span>
              )}
            </button>
          </div>

          {/* Filter & Refresh Controls */}
          <div className="flex items-center space-x-3">
            {contentTypeFilter && (
              <div className="flex items-center space-x-2 bg-white px-3 py-2 rounded-lg border border-gray-200">
                <Filter size={16} className="text-gray-500" />
                <span className="text-sm font-medium text-gray-700">
                  {contentTypeFilter}
                </span>
                <button
                  onClick={() => setContentTypeFilter('')}
                  className="text-gray-400 hover:text-gray-600"
                >
                  ×
                </button>
              </div>
            )}
            
            <button
              onClick={handleRefresh}
              disabled={currentLoading}
              className="bg-white hover:bg-gray-50 text-gray-600 px-4 py-2 rounded-lg border border-gray-200 transition-colors duration-200 flex items-center space-x-2"
            >
              <RefreshCw size={16} className={currentLoading ? 'animate-spin' : ''} />
              <span className="hidden sm:inline">Refresh</span>
            </button>
          </div>
        </div>

        {/* Content Header */}
        <div className="mb-6">
          <h2 className="text-3xl font-bold text-gray-900 mb-2 flex items-center">
            {activeTab === 'recommendations' ? (
              <>
                <TrendingUp className="mr-3 text-blue-600" />
                Personalized Recommendations
              </>
            ) : (
              <>
                {searchType === 'ai' ? (
                  <Sparkles className="mr-3 text-purple-600" />
                ) : (
                  <Search className="mr-3 text-blue-600" />
                )}
                Search Results
              </>
            )}
          </h2>
          
          <div className="flex flex-wrap items-center gap-3 text-sm text-gray-600">
            {activeTab === 'recommendations' && algorithm && (
              <span className="bg-blue-100 text-blue-800 px-3 py-1 rounded-full font-medium">
                {algorithm.replace('_', ' ').toUpperCase()}
              </span>
            )}
            
            {activeTab === 'search' && lastSearchQuery && (
              <span className="bg-purple-100 text-purple-800 px-3 py-1 rounded-full font-medium">
                {/* Bug #24 fix: use JSX expression, not literal quote characters */}
                &ldquo;{lastSearchQuery}&rdquo;
              </span>
            )}
            
            {contentTypeFilter && (
              <span className="bg-green-100 text-green-800 px-3 py-1 rounded-full font-medium">
                {contentTypeFilter}
              </span>
            )}
            
            <span className="flex items-center">
              <span className="w-2 h-2 bg-green-500 rounded-full mr-2"></span>
              {currentResults.length} items
            </span>
          </div>
        </div>

        {/* Error State */}
        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-6 py-4 rounded-xl mb-8 flex items-center">
            <AlertCircle className="mr-3 flex-shrink-0" size={20} />
            <div>
              <p className="font-medium">Error:</p>
              <p>{error}</p>
            </div>
          </div>
        )}

        {/* Loading State */}
        {currentLoading ? (
          <LoadingSpinner 
            loading={currentLoading}
            message={
              activeTab === 'search' 
                ? searchType === 'ai' 
                  ? "AI is analyzing your request..." 
                  : "Searching through content..."
                : "Loading personalized recommendations..."
            }
            searchType={activeTab === 'search' ? searchType : 'recommendations'}
          />
        ) : currentResults.length > 0 ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6 mb-12">
            {currentResults.map((item) => (
              <RecommendationCard
                key={item.item_id}
                item={item}
                onInteraction={logInteraction}
                onClick={handleItemClick}
                showScore={activeTab === 'search' || (activeTab === 'recommendations' && algorithm === 'xgboost_ml')}
              />
            ))}
          </div>
        ) : (
          <div className="text-center py-16">
            <div className="text-6xl mb-4">
              {activeTab === 'search' ? '🔍' : '🎯'}
            </div>
            <p className="text-gray-500 text-xl mb-2">
              {activeTab === 'search' 
                ? 'No search results found' 
                : 'No recommendations available'
              }
            </p>
            <p className="text-gray-400">
              {activeTab === 'search' 
                ? 'Try a different search query or change your filters'
                : 'Try changing the user or content filter'
              }
            </p>
          </div>
        )}

        {/* NEW-7 fix: only show Load More when there are more items to fetch */}
        {currentResults.length > 0 && (activeTab === 'recommendations' ? hasMoreRecs : hasMoreSearch) && (
          <div className="text-center py-8">
            <button
              onClick={handleLoadMore}
              disabled={currentLoading}
              className="bg-white hover:bg-gray-50 text-gray-700 px-8 py-3 rounded-xl shadow-md hover:shadow-lg transition-all duration-200 border border-gray-200 font-medium transform hover:scale-105 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <RefreshCw size={16} className={`inline mr-2 ${currentLoading ? 'animate-spin' : ''}`} />
              Load More {activeTab === 'search' ? 'Results' : 'Recommendations'}
            </button>
          </div>
        )}
      </div>

      {/* Item Modal */}
      <ItemModal
        item={selectedItem}
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onInteraction={logInteraction}
      />

      {/* NEW-4 fix: React-managed toast notifications */}
      <div className="fixed top-4 right-4 z-50 flex flex-col gap-2 pointer-events-none">
        {notifications.map(n => (
          <div
            key={n.id}
            className={`px-6 py-3 rounded-lg shadow-lg text-white font-medium transition-all duration-300 pointer-events-auto ${
              n.type === 'success' ? 'bg-green-500' : 'bg-red-500'
            }`}
          >
            {n.message}
          </div>
        ))}
      </div>

      {/* LOW-2 fix: unified product name to RecommendAI in footer */}
      <footer className="bg-white border-t border-gray-200 mt-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <div className="text-center">
            <div className="bg-gradient-to-r from-blue-500 to-purple-600 p-3 rounded-xl inline-block mb-4">
              <Sparkles className="text-white" size={32} />
            </div>
            <h3 className="text-2xl font-bold text-gray-900 mb-3">
              RecommendAI
            </h3>
            <p className="text-gray-600 mb-6 max-w-2xl mx-auto">
              Powered by XGBoost ML, A/B Testing, AI Search, and Real-time Event Processing. 
              Experience personalized content discovery at scale with natural language understanding.
            </p>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center max-w-4xl mx-auto">
              <div className="bg-blue-50 p-4 rounded-xl">
                <div className="text-2xl mb-2">🤖</div>
                <div className="text-sm font-medium text-blue-900">ML-Powered</div>
              </div>
              <div className="bg-green-50 p-4 rounded-xl">
                <div className="text-2xl mb-2">⚡</div>
                <div className="text-sm font-medium text-green-900">Real-time</div>
              </div>
              <div className="bg-purple-50 p-4 rounded-xl">
                <div className="text-2xl mb-2">🔍</div>
                <div className="text-sm font-medium text-purple-900">AI Search</div>
              </div>
              <div className="bg-orange-50 p-4 rounded-xl">
                <div className="text-2xl mb-2">🧪</div>
                <div className="text-sm font-medium text-orange-900">A/B Testing</div>
              </div>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default App;
