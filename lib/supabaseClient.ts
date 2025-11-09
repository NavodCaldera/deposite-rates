import { createClient } from '@supabase/supabase-js'

// These variables are pulled from your .env.local file
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

// The '!' tells TypeScript that we are sure these values will exist.
// You should add runtime checks if you're unsure.

export const supabase = createClient(supabaseUrl, supabaseAnonKey)