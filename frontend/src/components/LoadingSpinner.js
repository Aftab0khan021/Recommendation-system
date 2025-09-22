import React from 'react';
import { Loader2, Search, Sparkles } from 'lucide-react';

const LoadingSpinner = ({ 
  loading = false, 
  message = "Loading amazing recommendations...",
  searchType = "simple",
  size = "large" 
}) => {
  if (!loading) return null;

  const sizeClasses = {
    small: { container: "py-8", spinner: "h-8 w-8", text: "text-sm" },
    medium: { container: "py-12", spinner: "h-12 w-12", text: "text-base" },
    large: { container: "py-16", spinner: "h-16 w-16", text: "text-lg" }
  };

  const classes = sizeClasses[size] || sizeClasses.large;

  return (
    <div className={`flex justify-center items-center ${classes.container}`}>
      <div className="text-center">
        {/* Animated Spinner */}
        <div className="relative mb-4">
          {searchType === "ai" ? (
            <div className="relative">
              <Sparkles 
                className={`${classes.spinner} text-purple-600 mx-auto animate-pulse`} 
              />
              <div className="absolute inset-0 animate-spin">
                <div className={`${classes.spinner} border-4 border-purple-200 border-t-purple-600 rounded-full`}></div>
              </div>
            </div>
          ) : searchType === "search" ? (
            <div className="relative">
              <Search 
                className={`${classes.spinner} text-blue-600 mx-auto animate-pulse`} 
              />
              <div className="absolute inset-0 animate-spin">
                <div className={`${classes.spinner} border-4 border-blue-200 border-t-blue-600 rounded-full`}></div>
              </div>
            </div>
          ) : (
            <div className="relative">
              <Loader2 
                className={`${classes.spinner} text-blue-600 mx-auto animate-spin`} 
              />
              <div className="absolute inset-0 animate-pulse">
                <div className={`${classes.spinner} bg-gradient-to-r from-blue-400 to-purple-400 rounded-full opacity-20`}></div>
              </div>
            </div>
          )}
        </div>

        {/* Loading Message */}
        <p className={`text-gray-600 ${classes.text} font-medium`}>
          {message}
        </p>

        {/* Loading Dots Animation */}
        <div className="flex justify-center space-x-1 mt-3">
          <div className="w-2 h-2 bg-blue-500 rounded-full animate-bounce"></div>
          <div className="w-2 h-2 bg-purple-500 rounded-full animate-bounce" style={{animationDelay: '0.1s'}}></div>
          <div className="w-2 h-2 bg-pink-500 rounded-full animate-bounce" style={{animationDelay: '0.2s'}}></div>
        </div>

        {/* Additional Context based on search type */}
        {searchType === "ai" && (
          <p className="text-sm text-purple-600 mt-2 flex items-center justify-center">
            <Sparkles size={14} className="mr-1" />
            AI is understanding your request...
          </p>
        )}
        
        {searchType === "search" && (
          <p className="text-sm text-blue-600 mt-2 flex items-center justify-center">
            <Search size={14} className="mr-1" />
            Searching through content...
          </p>
        )}
      </div>
    </div>
  );
};

export default LoadingSpinner;