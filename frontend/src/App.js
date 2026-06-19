import React, { useState, useEffect, useCallback, useRef } from 'react';
import axios from 'axios';
import './App.css';

// Components
import Navigation from './components/Navigation';
import SearchBar from './components/SearchBar';
import StatsDashboard from './components/StatsDashboard';
import RecommendationCard from './components/RecommendationCard';
import ItemModal from './components/ItemModal';
import { SkeletonGrid } from './components/SkeletonCard';
import FilterPanel from './components/FilterPanel';        // Phase 2
import ProfilePage from './pages/ProfilePage';             // Phase 2
import ExperimentsPage from './pages/ExperimentsPage';     // Phase 2+3

import { AlertCircle, SlidersHorizontal } from 'lucide-react';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://127.0.0.1:8000';
const API = `${BACKEND_URL}/api`;

const App = () => {
  // ── Core state ─────────────────────────────────────────────
  const [currentUser, setCurrentUser] = useState('demo_user_1');
  const [recommendations, setRecommendations] = useState([]);
  const [searchResults, setSearchResults] = useState([]);
  const [contentTypeFilter, setContentTypeFilter] = useState('');
  const [sortBy, setSortBy] = useState('relevance');    // Phase 2
  const [searchType, setSearchType] = useState('simple');
  const [loading, setLoading] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [stats, setStats] = useState(null);
  const [algorithm, setAlgorithm] = useState('');
  const [error, setError] = useState(null);
  const [selectedItem, setSelectedItem] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [activeTab, setActiveTab] = useState('recommendations');
  const [lastSearchQuery, setLastSearchQuery] = useState('');
  const [recPage, setRecPage] = useState(1);
  const [searchPage, setSearchPage] = useState(1);
  const [hasMoreRecs, setHasMoreRecs] = useState(true);
  const [hasMoreSearch, setHasMoreSearch] = useState(true);
  const [notifications, setNotifications] = useState([]);
  // Phase 2: dark mode + filter panel
  const [darkMode, setDarkMode] = useState(() => {
    try { return localStorage.getItem('darkMode') === 'true'; } catch { return false; }
  });
  const [filterPanelOpen, setFilterPanelOpen] = useState(false);
  // Phase 2: infinite scroll sentinel
  const sentinelRef = useRef(null);
  const PAGE_SIZE = 20;

  // Apply dark mode class to <html>
  useEffect(() => {
    document.documentElement.classList.toggle('dark', darkMode);
    try { localStorage.setItem('darkMode', String(darkMode)); } catch {}
  }, [darkMode]);

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

  // Phase 2: Infinite scroll via IntersectionObserver
  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && !currentLoading) {
          if (activeTab === 'recommendations' && hasMoreRecs) {
            const next = recPage + 1;
            setRecPage(next);
            fetchRecommendations(next, true);
          } else if (activeTab === 'search' && hasMoreSearch && lastSearchQuery) {
            const next = searchPage + 1;
            setSearchPage(next);
            handleSearch(lastSearchQuery, next, true);
          }
        }
      },
      { rootMargin: '200px' }  // trigger 200px before the sentinel enters viewport
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [currentLoading, activeTab, hasMoreRecs, hasMoreSearch, recPage, searchPage, lastSearchQuery,
      fetchRecommendations, handleSearch]);

  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Navigation */}
      <Navigation
        currentUser={currentUser}
        onUserChange={setCurrentUser}
        onRefresh={handleRefresh}
        loading={currentLoading}
        stats={stats}
        darkMode={darkMode}
        onDarkModeToggle={() => setDarkMode(d => !d)}
        activeTab={activeTab}
        onTabChange={setActiveTab}
      />

      {/* Main layout: filter sidebar + content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 flex gap-6">

        {/* Phase 2: FilterPanel (only on recommendations/search tabs) */}
        {(activeTab === 'recommendations' || activeTab === 'search') && (
          <FilterPanel
            contentTypeFilter={contentTypeFilter}
            onContentTypeChange={(val) => { setContentTypeFilter(val); setRecPage(1); setRecommendations([]); }}
            sortBy={sortBy}
            onSortChange={setSortBy}
            isOpen={filterPanelOpen}
            onClose={() => setFilterPanelOpen(false)}
          />
        )}

        {/* Content column */}
        <div className="flex-1 min-w-0">

          {/* Search Bar */}
          {(activeTab === 'recommendations' || activeTab === 'search') && (
            <div className="mb-6">
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
          )}

          {/* Stats Dashboard */}
          {(activeTab === 'recommendations' || activeTab === 'search') && (
            <StatsDashboard stats={stats} className="mb-6" />
          )}

          {/* Page: Profile */}
          {activeTab === 'profile' && (
            <div className="page-enter">
              <ProfilePage userId={currentUser} />
            </div>
          )}

          {/* Page: Experiments */}
          {activeTab === 'experiments' && (
            <div className="page-enter">
              <ExperimentsPage userId={currentUser} />
            </div>
          )}

          {/* Main Content (Recommendations + Search tabs) */}
          {(activeTab === 'recommendations' || activeTab === 'search') && (
            <>
              {/* Controls row */}
              <div className="flex items-center justify-between mb-4">
                <div className="flex flex-wrap items-center gap-2 text-sm">
                  {activeTab === 'recommendations' && algorithm && (
                    <span className="bg-primary/10 text-primary px-3 py-1 rounded-full font-medium border border-primary/20">
                      {algorithm.replace(/_/g, ' ').toUpperCase()}
                    </span>
                  )}
                  {activeTab === 'search' && lastSearchQuery && (
                    <span className="bg-secondary/10 text-secondary px-3 py-1 rounded-full font-medium border border-secondary/20">
                      &ldquo;{lastSearchQuery}&rdquo;
                    </span>
                  )}
                  {contentTypeFilter && (
                    <span className="bg-accent/10 text-accent px-3 py-1 rounded-full font-medium border border-accent/20">
                      {contentTypeFilter}
                    </span>
                  )}
                  <span className="flex items-center gap-1 text-muted-foreground">
                    <span className="live-dot w-1.5 h-1.5 rounded-full bg-green-400" />
                    {currentResults.length} items
                  </span>
                </div>

                {/* Filter Panel toggle */}
                <button
                  onClick={() => setFilterPanelOpen(o => !o)}
                  className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm transition-colors border ${
                    filterPanelOpen
                      ? 'bg-primary text-primary-foreground border-primary'
                      : 'bg-muted text-muted-foreground border-border hover:text-foreground'
                  }`}
                >
                  <SlidersHorizontal size={14} />
                  Filters
                  {(contentTypeFilter || sortBy !== 'relevance') && (
                    <span className="w-1.5 h-1.5 rounded-full bg-current" />
                  )}
                </button>
              </div>

              {/* Error */}
              {error && (
                <div className="bg-destructive/10 border border-destructive/30 text-destructive px-6 py-4 rounded-xl mb-6 flex items-center gap-3">
                  <AlertCircle size={20} className="flex-shrink-0" />
                  <p>{error}</p>
                </div>
              )}

              {/* Loading State: skeleton grid */}
              {currentLoading && recommendations.length === 0 ? (
                <SkeletonGrid count={8} />
              ) : currentResults.length > 0 ? (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-5 mb-8">
                  {currentResults.map((item) => (
                    <RecommendationCard
                      key={item.item_id}
                      item={item}
                      onInteraction={logInteraction}
                      onClick={handleItemClick}
                      showScore={activeTab === 'search' || algorithm === 'xgboost_ml'}
                    />
                  ))}
                </div>
              ) : !currentLoading ? (
                <div className="text-center py-20">
                  <div className="text-6xl mb-4">{activeTab === 'search' ? '🔍' : '🎯'}</div>
                  <p className="text-foreground text-xl mb-2 font-medium">
                    {activeTab === 'search' ? 'No results found' : 'No recommendations yet'}
                  </p>
                  <p className="text-muted-foreground">
                    {activeTab === 'search'
                      ? 'Try a different search query'
                      : 'Interact with items to personalise your feed'
                    }
                  </p>
                </div>
              ) : null}

              {/* Phase 2: Infinite scroll sentinel */}
              {currentResults.length > 0 && (activeTab === 'recommendations' ? hasMoreRecs : hasMoreSearch) && (
                <div ref={sentinelRef} className="flex justify-center py-8">
                  {currentLoading && (
                    <div className="flex gap-1.5">
                      {[0,1,2].map(i => (
                        <div key={i}
                          className="w-2.5 h-2.5 rounded-full bg-primary animate-bounce"
                          style={{ animationDelay: `${i * 0.15}s` }}
                        />
                      ))}
                    </div>
                  )}
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {/* Item Modal */}
      <ItemModal
        item={selectedItem}
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onInteraction={logInteraction}
      />

      {/* Toast Notifications */}
      <div className="fixed top-4 right-4 z-50 flex flex-col gap-2 pointer-events-none">
        {notifications.map(n => (
          <div
            key={n.id}
            className={`notification-toast px-6 py-3 rounded-xl shadow-xl text-white font-medium pointer-events-auto ${
              n.type === 'success' ? 'bg-green-500' : 'bg-destructive'
            }`}
          >
            {n.message}
          </div>
        ))}
      </div>

      {/* Premium Footer */}
      <footer className="bg-card border-t border-border mt-16">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <div className="text-center">
            <div className="bg-gradient-to-r from-blue-500 to-purple-600 p-3 rounded-xl inline-block mb-4 glow-blue">
              <span className="text-white text-2xl">✨</span>
            </div>
            <h3 className="text-2xl font-bold gradient-text mb-3">RecommendAI</h3>
            <p className="text-muted-foreground mb-6 max-w-2xl mx-auto">
              Powered by XGBoost ML · ALS Collaborative Filtering · TF-IDF Vector Search ·
              Thompson Sampling A/B Testing · Real-time Event Processing
            </p>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center max-w-4xl mx-auto">
              {[
                { emoji: '🤖', label: 'ML-Powered', desc: 'XGBoost + ALS' },
                { emoji: '⚡', label: 'Real-time', desc: 'Instant cache' },
                { emoji: '🔍', label: 'Vector Search', desc: 'TF-IDF semantic' },
                { emoji: '🧪', label: 'Bandit A/B', desc: 'Thompson Sampling' },
              ].map(({ emoji, label, desc }) => (
                <div key={label} className="glass-card p-4 rounded-xl card-hover">
                  <div className="text-2xl mb-2">{emoji}</div>
                  <div className="text-sm font-semibold text-foreground">{label}</div>
                  <div className="text-xs text-muted-foreground mt-0.5">{desc}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default App;
