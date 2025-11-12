'use client'; 

import React, { useState, useEffect, useMemo } from 'react';

// Define the structure of a single rate object
interface Rate {
  id: number;
  bankName: string;
  fdType: string;
  institutionType: string; // <-- NEW
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

// --- NEW: Helper function to get unique values for filters ---
function getUniqueValues(rates: Rate[], key: keyof Rate): string[] {
  // Creates a Set (which only holds unique values) from a list of all values,
  // then converts it back to an array and sorts it.
  return Array.from(new Set(rates.map(rate => rate[key])))
    .filter(Boolean) // Remove any null/undefined
    .sort() as string[];
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
  const [sortBy, setSortBy] = useState<'finalPayout' | 'termMonths'>('finalPayout');
  
  // --- NEW: State for our new filters ---
  const [amount, setAmount] = useState<number>(100000); // Default LKR 100,000
  const [fdType, setFdType] = useState<string>('All');
  const [payoutSchedule, setPayoutSchedule] = useState<string>('All');
  const [institutionType, setInstitutionType] = useState<string>('All'); // <-- NEW

  // --- NEW: State for our filter dropdown options ---
  const [fdTypeOptions, setFdTypeOptions] = useState<string[]>([]);
  const [payoutScheduleOptions, setPayoutScheduleOptions] = useState<string[]>([]);
  const [institutionTypeOptions, setInstitutionTypeOptions] = useState<string[]>([]); // <-- NEW


  // --- Data Fetching ---
  useEffect(() => {
    async function fetchData() {
      setIsLoading(true);
      setError(null);
      try {
        const response = await fetch('/api/rates', { cache: 'no-store' }); 
        
        if (!response.ok) {
          throw new Error(`Failed to fetch data: Server responded with ${response.status}`);
        }
        
        const data = await response.json();

        if (Array.isArray(data)) {
          setRates(data);
          // --- NEW: Once data is loaded, populate all our filter dropdowns ---
          setFdTypeOptions(['All', ...getUniqueValues(data, 'fdType')]);
          setPayoutScheduleOptions(['All', ...getUniqueValues(data, 'payoutSchedule')]);
          setInstitutionTypeOptions(['All', ...getUniqueValues(data, 'institutionType')]); // <-- NEW
        } else {
          throw new Error('Failed to fetch data: API did not return an array.');
        }

      } catch (err) {
        if (err instanceof Error) {
          setError(err.message);
        } else {
          setError('An unknown error occurred');
        }
      } finally {
        setIsLoading(false);
      }
    }
    fetchData();
  }, []); 

  // --- Filtering and Sorting Logic ---
  const filteredAndSortedRates = useMemo(() => {
    const calculatedRates = rates.map(rate => {
      // --- NEW: Calculate the final payout for each rate ---
      const termInYears = rate.termMonths / 12;
      // Use AER if available (it's the true rate), otherwise fall back to interestRate
      const effectiveRate = rate.aer || rate.interestRate;
      
      const finalPayout = amount * Math.pow(1 + (effectiveRate / 100), termInYears);
      
      return {
        ...rate,
        finalPayout: finalPayout,
      };
    });

    return calculatedRates
      .filter(rate => 
        rate.bankName.toLowerCase().includes(searchTerm.toLowerCase())
      )
      .filter(rate => 
        minTerm === 0 ? true : rate.termMonths >= minTerm
      )
      // --- NEW: Filter by FD Type ---
      .filter(rate => 
        fdType === 'All' ? true : rate.fdType === fdType
      )
      // --- NEW: Filter by Payout Schedule ---
      .filter(rate =>
        payoutSchedule === 'All' ? true : rate.payoutSchedule === payoutSchedule
      )
      // --- NEW: Filter by Institution Type ---
      .filter(rate =>
        institutionType === 'All' ? true : rate.institutionType === institutionType
      )
      .sort((a, b) => {
        // --- UPDATED: Sort by our new 'finalPayout' value ---
        if (sortBy === 'finalPayout') {
          return (b.finalPayout || 0) - (a.finalPayout || 0);
        }
        return a.termMonths - b.termMonths;
      });
  }, [rates, searchTerm, minTerm, sortBy, amount, fdType, payoutSchedule, institutionType]); // Add new dependencies

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-4 sm:p-8">
      <div className="max-w-7xl mx-auto">
        
        <header className="mb-8">
          <h1 className="text-4xl font-bold text-white text-center mb-2">
            FD Rate Calculator
          </h1>
          <p className="text-lg text-gray-400 text-center">
            Find the best Fixed Deposit for your goals in Sri Lanka.
          </p>
        </header>

        {/* --- UPDATED: Filter Controls Section --- */}
        <div className="mb-6 p-4 bg-gray-800 rounded-lg shadow-md flex flex-col gap-4">
          {/* Row 1: Amount and Search */}
          <div className="flex flex-col sm:flex-row gap-4">
            <div className="flex-1 sm:flex-grow-0 sm:w-1/3">
              <label htmlFor="amount" className="block text-sm font-medium text-gray-300 mb-1">
                Investment Amount (LKR)
              </label>
              <input
                type="number"
                id="amount"
                className="w-full p-3 bg-gray-700 rounded-md border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 text-white"
                value={amount}
                onChange={(e) => setAmount(Number(e.target.value) || 0)}
              />
            </div>
            <div className="flex-1 sm:flex-grow-2 sm:w-2/3">
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
          </div>
          
          {/* Row 2: Filters and Sort */}
          <div className="flex flex-col sm:flex-row gap-4">
            {/* --- NEW: Institution Type Filter --- */}
            <div className="flex-1">
              <label htmlFor="institutionType" className="block text-sm font-medium text-gray-300 mb-1">
                Institution Type
              </label>
              <select
                id="institutionType"
                className="w-full p-3 bg-gray-700 rounded-md border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 text-white appearance-none"
                value={institutionType}
                onChange={(e) => setInstitutionType(e.target.value)}
              >
                {institutionTypeOptions.map(option => <option key={option} value={option}>{option}</option>)}
              </select>
            </div>
            <div className="flex-1">
              <label htmlFor="fdType" className="block text-sm font-medium text-gray-300 mb-1">
                FD Type
              </label>
              <select
                id="fdType"
                className="w-full p-3 bg-gray-700 rounded-md border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 text-white appearance-none"
                value={fdType}
                onChange={(e) => setFdType(e.target.value)}
              >
                {fdTypeOptions.map(option => <option key={option} value={option}>{option}</option>)}
              </select>
            </div>
            <div className="flex-1">
              <label htmlFor="payoutSchedule" className="block text-sm font-medium text-gray-300 mb-1">
                Payout Schedule
              </label>
              <select
                id="payoutSchedule"
                className="w-full p-3 bg-gray-700 rounded-md border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 text-white appearance-none"
                value={payoutSchedule}
                onChange={(e) => setPayoutSchedule(e.target.value)}
              >
                {payoutScheduleOptions.map(option => <option key={option} value={option}>{option}</option>)}
              </select>
            </div>
            <div className="flex-1">
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
            <div className="flex-1">
              <label htmlFor="sort" className="block text-sm font-medium text-gray-300 mb-1">
                Sort By
              </label>
              <select
                id="sort"
                className="w-full p-3 bg-gray-700 rounded-md border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 text-white appearance-none"
                value={sortBy}
                onChange={(e) => setSortBy(e.target.value as 'finalPayout' | 'termMonths')}
              >
                <option value="finalPayout">Highest Final Payout</option>
                <option value="termMonths">Shortest Term</option>
              </select>
            </div>
          </div>
        </div>

        {/* Data Display Area (Table) */}
        <main className="bg-gray-800 rounded-lg shadow-lg overflow-hidden">
          {isLoading && <LoadingSpinner />}
          {error && (
            <div className="p-10 text-center text-red-400">
              <h3 className="text-xl font-semibold">An Error Occurred</h3>
              <p>{error}</p>
            </div>
          )}
          
          {!isLoading && !error && (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-700">
                <thead className="bg-gray-700">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                      Bank / Finance Co.
                    </th>
                    {/* --- NEW: Table Header --- */}
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                      Type
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
                    <th className="px-6 py-3 text-right text-xs font-medium text-white uppercase tracking-wider border-l border-gray-600">
                      Total Payout (LKR)
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-gray-800 divide-y divide-gray-700">
                  {filteredAndSortedRates.length === 0 ? (
                    <tr>
                      {/* --- UPDATED: Colspan --- */}
                      <td colSpan={8} className="px-6 py-10 text-center text-gray-400">
                        No results found for your filter.
                      </td>
                    </tr>
                  ) : (
                    filteredAndSortedRates.map((rate, index) => (
                      <tr key={`${rate.bankName}-${rate.termMonths}-${rate.payoutSchedule}-${index}`} className="hover:bg-gray-700 transition-colors">
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm font-medium text-white">{rate.bankName}</div>
                        </td>
                         {/* --- NEW: Table Data Cell --- */}
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                            rate.institutionType === 'Bank'
                              ? 'bg-green-900 text-green-200' 
                              : 'bg-purple-900 text-purple-200'
                          }`}>
                            {rate.institutionType}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm text-gray-300">{rate.termMonths} Months</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm text-gray-300">{rate.payoutSchedule}</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                            rate.fdType.toLowerCase().includes('senior') 
                              ? 'bg-green-900 text-green-200' 
                              : 'bg-blue-900 text-blue-200'
                          }`}>
                            {rate.fdType}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm text-gray-400">{rate.interestRate.toFixed(2)}%</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm text-gray-400">{rate.aer ? `${rate.aer.toFixed(2)}%` : '-'}</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right text-lg font-bold text-green-400 border-l border-gray-700 bg-gray-900">
                          {rate.finalPayout.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          )}
        </main>
        
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