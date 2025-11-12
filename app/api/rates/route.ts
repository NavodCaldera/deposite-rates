import { getClient } from '../../../lib/supabaseClient'; // <-- Uses the new 'getClient' function
import { NextResponse } from 'next/server';

// This line forces Vercel to *never* cache this API.
export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    // 1. Create the Supabase client using our new function
    const supabase = getClient(); // <-- Uses 'getClient'
    
    // 2. Fetch all data from the 'public-rates' table
    const { data, error } = await supabase
      .from('public-rates') 
      .select('*') 
      .order('interestRate', { ascending: false }); 

    // 3. If Supabase gave an error, throw it
    if (error) {
      throw new Error(error.message);
    }

    // 4. Send the data back as a direct array
    return NextResponse.json(data);

  } catch (error) {
    let errorMessage = 'An unknown error occurred';
    if (error instanceof Error) {
      errorMessage = error.message;
    }
    // 5. Send an error message if anything went wrong
    return NextResponse.json({ error: errorMessage }, { status: 500 });
  }
}