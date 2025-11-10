import { NextResponse } from 'next/server';
import { supabase } from '@/lib/supabaseClient'; // Adjust path if needed

export const dynamic = 'force-dynamic';

export async function GET(request: Request) { // Added 'Request' type
  try {
    // Select all data from the 'public-rates' table
    const { data, error } = await supabase
      .from('public-rates')
      .select('*')
      .order('bankName', { ascending: true })
      .order('termMonths', { ascending: true });

    if (error) {
      // If Supabase throws an error
      console.error('Supabase error:', error);
      return NextResponse.json(
        { message: "Error fetching data from database", error: error.message },
        { status: 500 }
      );
    }

    // Success! Return the data
    return NextResponse.json({ rates: data }, { status: 200 });

  } catch (e: any) { // Added 'any' type for the catch block
    // For any other unexpected errors
    console.error('Unexpected API error:', e);
    return NextResponse.json(
      { message: "An unexpected error occurred", error: e.message },
      { status: 500 }
    );
  }
}