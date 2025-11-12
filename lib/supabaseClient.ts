import { createClient } from '@supabase/supabase-js';

// These variables are loaded from your .env.local file (for local)
// or from Vercel's Environment Variables (for production)
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;

// We are RENAMING this function to 'getClient' to avoid the name conflict
export const getClient = () => {
  return createClient(supabaseUrl, supabaseAnonKey);
};