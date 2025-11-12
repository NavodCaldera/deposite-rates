'use client'; 

import React, { useState, useEffect, useMemo } from 'react';

// Define the structure of a single rate object
interface Rate {
  id: number;
  bankName: string;
  fdType: string;
  termMonths: number;
  payoutSchedule: string;
  interestRate: number;
  aer: number | null;
}

// A small, reusable component for the loading spinner
function LoadingSpinner() {
  return (
    <div className="flex justify-center items-center py-10">
      <div className="w-12 h-12 border-4 border-t-blue-500 border-gray-200 rounded-full animate-spin"></div>
    </div>
  );
}

// This is the main function for your page
export default function RateAggregatorPage() {
  // --- State Variables ---
  const [rates, setRates] = useState<Rate[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  // --- Filter and Sort State ---
  const [searchTerm, setSearchTerm] = useState('');
  const [minTerm, setMinTerm] = useState<number>(0);
  const [sortBy, setSortBy] = useState<'interestRate' | 'termMonths'>('interestRate');

  // --- Data Fetching ---
  // This hook runs once when the component first loads
  useEffect(() => {
    async function fetchData() {
      setIsLoading(true);
      setError(null);
      try {
        // This command forces the browser to *never* use a cached version
        const response = await fetch('/api/rates', { cache: 'no-store' }); 
        
        if (!response.ok) {
          // If the server responded with 404, 500, etc.
          throw new Error(`Failed to fetch data: Server responded with ${response.status}`);
        }
        
        const data = await response.json();

        // This is the fix for your "rates.filter is not a function" error
        if (Array.isArray(data)) {
          setRates(data); // Save the array of rates
        } else {
          // This happens if the API sent back an error object
          throw new Error('Failed to fetch data: API did not return an array.');
        }

      } catch (err) {
        if (err instanceof Error) {
          setError(err.message);
        } else {
          setError('An unknown error occurred');
        }
      } finally {
        setIsLoading(false); // Hide spinner
      }
    }
    fetchData();
  }, []); // The empty array [] means "run this only once"

  // --- Filtering and Sorting Logic ---
  // This re-calculates the list every time a filter changes
  const filteredAndSortedRates = useMemo(() => {
    return rates
      .filter(rate => 
        rate.bankName.toLowerCase().includes(searchTerm.toLowerCase())
      )
      .filter(rate => 
        minTerm === 0 ? true : rate.termMonths >= minTerm
      )
      .sort((a, b) => {
        if (sortBy === 'interestRate') {
          return (b.interestRate || 0) - (a.interestRate || 0);
        }
        return a.termMonths - b.termMonths;
      });
  }, [rates, searchTerm, minTerm, sortBy]);

  // --- JSX (The HTML part) ---
  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-4 sm:p-8">
      <div className="max-w-7xl mx-auto">
        
        {/* Header Section */}
        <header className="mb-8">
          <h1 className="text-4xl font-bold text-white text-center mb-2">
            FD Rate Aggregator
          </h1>
          <p className="text-lg text-gray-400 text-center">
            Your daily updated guide to Fixed Deposit rates in Sri Lanka.
          </p>
        </header>

        {/* Filter Controls Section */}
        <div className="mb-6 p-4 bg-gray-800 rounded-lg shadow-md flex flex-col sm:flex-row gap-4">
          {/* Search by Bank */}
          <div className="flex-1">
            <label htmlFor="search" className="block text-sm font-medium text-gray-300 mb-1">
              Search by Bank
            </label>
            <input
              type="text"
              id="search"
              placeholder="E.g., Alliance Finance, HNB..."
              className="w-full p-3 bg-gray-700 rounded-md border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 text-white"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
          
          {/* Filter by Term */}
          <div className="flex-1 sm:flex-none">
            <label htmlFor="term" className="block text-sm font-medium text-gray-300 mb-1">
              Minimum Term
            </label>
            <select
              id="term"
              className="w-full p-3 bg-gray-700 rounded-md border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 text-white appearance-none"
              value={minTerm}
              onChange={(e) => setMinTerm(Number(e.target.value))}
            >
              <option value={0}>All Terms</option>
              <option value={3}>3+ Months</option>
              <option value={6}>6+ Months</option>
              <option value={12}>12+ Months (1 Year)</option>
              <option value={24}>24+ Months (2 Years)</option>
            </select>
          </div>
          
          {/* Sort By */}
          <div className="flex-1 sm:flex-none">
            <label htmlFor="sort" className="block text-sm font-medium text-gray-300 mb-1">
              Sort By
            </label>
            <select
              id="sort"
              className="w-full p-3 bg-gray-700 rounded-md border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 text-white appearance-none"
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value as 'interestRate' | 'termMonths')}
            >
              <option value="interestRate">Highest Interest Rate</option>
              <option value="termMonths">Shortest Term</option>
            </select>
          </div>
        </div>

        {/* Data Display Area (Table) */}
        <main className="bg-gray-800 rounded-lg shadow-lg overflow-hidden">
          {/* Show spinner while loading */}
          {isLoading && <LoadingSpinner />}
          
          {/* Show error message if something went wrong */}
          {error && (
            <div className="p-10 text-center text-red-400">
              <h3 className="text-xl font-semibold">An Error Occurred</h3>
              <p>{error}</p>
            </div>
          )}
          
          {/* Show the table ONLY if loading is finished and there is no error */}
          {!isLoading && !error && (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-700">
                <thead className="bg-gray-700">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                      Bank / Finance Co.
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                      Term
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                      Payout
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                      FD Type
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-300 uppercase tracking-wider">
                      Interest Rate (p.a.)
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-300 uppercase tracking-wider">
                      AER
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-gray-800 divide-y divide-gray-700">
                  {/* Show a message if no filters match */}
                  {filteredAndSortedRates.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-6 py-10 text-center text-gray-400">
                        No results found for your filter.
                      </td>
                    </tr>
                  ) : (
                    // Loop over the data and create a table row for each rate
                    filteredAndSortedRates.map((rate, index) => (
                      <tr key={`${rate.bankName}-${rate.termMonths}-${rate.payoutSchedule}-${index}`} className="hover:bg-gray-700 transition-colors">
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm font-medium text-white">{rate.bankName}</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm text-gray-300">{rate.termMonths} Months</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm text-gray-300">{rate.payoutSchedule}</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          {/* Special styling for Senior Citizen rates */}
                          <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                            rate.fdType.toLowerCase().includes('senior') 
                              ? 'bg-green-900 text-green-200' 
                              : 'bg-blue-900 text-blue-200'
                          }`}>
                            {rate.fdType}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-base font-bold text-green-400">{rate.interestRate.toFixed(2)}%</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm text-gray-400">{rate.aer ? `${rate.aer.toFixed(2)}%` : '-'}</div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          )}
        </main>
        
        {/* Footer Section */}
        <footer className="mt-8 text-center text-gray-500 text-sm">
          <p>
            Disclaimer: Rates are scraped automatically and are for informational purposes only.
            Always verify with the financial institution before making a decision.
          </p>
        </footer>
      </div>
    </div>
  );
}