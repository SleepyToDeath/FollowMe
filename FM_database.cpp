#include"FM_database.h"
#include<sstream>

using namespace FM_datatbase;
using std::string;
using std::vector;
using std::stringstream;
using std::fstream;
using std::ios;

string itos(index_t i)
{
	stringstream ss;
	ss<<i;
	return ss.str();
}

database::database()
{
	fstream fin(init_file_name);
	string s;
	getline(fin,s);
	if (s.find("cache_size")) 
	{
		size_t pos=s.find("=");
		db_meta_0.cache_size=atoi(s.substr(pos+1,s.length()-pos-1).c_str());
	}
	else if (s.find("cache_capacity")) 
	{
		size_t pos=s.find("=");
		db_meta_0.cache_capacity=atoi(s.substr(pos+1,s.length()-pos-1).c_str());
	}
	else if (s.find("block_size")) 
	{
		size_t pos=s.find("=");
		db_meta_0.block_size=atoi(s.substr(pos+1,s.length()-pos-1).c_str());
	}

}

/* table & cache */

index_t database::new_table( index_t len )
{
	table* table_0=new table;
	tables.push_back(table_0);
	table_0->dataio.open((itos(tables.size()-1)+".dat").c_str() , ios::binary | ios::in | ios::out );
	fstream metaio((itos(tables.size()-1)+".meta").c_str() , ios::binary | ios::in | ios::out );

}


index_t database::add( index_t handler , char* value , std::vector<std::string> keys )
{
	table* table_0=tables[ handler ];

	entry tmp;
	tmp.keys = keys;
	tmp.value = string( value );
	tmp.index = table_0->table_meta_0.max_order;
	table_0->table_meta_0.max_order++;
	tmp.keys.push_back( tmp.index );

	cache_entry tmp_2;
	tmp_2.entry_0 = tmp;
	string tmp_ck = tmp.keys[ table_0->table_meta_0.central_key ];
	if ( table_0->hash_0.find( tmp_ck ) )
	{
		tmp_2.next = table_0->hash_0[ tmp_ck ];
		table_0->hash_0[ tmp_ck ] = tmp.index;
		heap_inc( handler , tmp_ck , 1 );
	}
	else
	{
		table_0->hash_0.add( tmp_ck , tmp.index );
		heap_add( handler , tmp_ck , 1 );
		tmp_2.next = -1;
	}
	table_0->cache.add( tmp.index , tmp_2 );

	if ( table_0->cache.count() >= cache_capacity )
	{
		index i = table_0->hash_0[ heap_min( handler ) ];
		while ( i >= 0 )
		{
			


	

}

index_t database::del( index_t handler , index_t index )
{
	
}

string get( index_t handler , index_t index )
{


}

/* heap */

void database::swap_heap_entry( index_t handler , index_t i , index_t j )
{
	table* table_0 = tables[handler];
	table_0->hash_0[table_0->heap[i].key] = j;
	table_0->hash_0[table_0->heap[j].key] = i;
	heap_entry tmp = table_0->heap[j];
	table_0->heap[j] = table_0->heap[i];
	table_0->heap[i] = tmp;
}

void database::heap_up( index_t handler , index_t pos )
{
	table* table_0=tables[handler];
	index_t i=pos/2;
	if (i>0)
		if ( table_0->heap[i].count > table_0->heap[pos].count )
		{
			swap_heap_entry( handler , pos , i );
			heap_up( handler , i );
		}
}

void database::heap_down( index_t handler , index_t pos )
{
	table* table_0=tables[handler];
	index_t i=pos;
	if (pos*2 <= (long long)table_0->heap.size()-1)
	{
		if ( table_0->heap[pos].count > table_0->heap[pos*2].count )
			i=pos*2;
		if ( pos*2+1 <= (long long)table_0->heap.size()-1 &&
			table_0->heap[i].count > table_0->heap[pos*2+1].count )
			i=pos*2+1;
	}
	if ( i>pos )
	{
		swap_heap_entry( handler , pos , i );
		heap_down( handler , i );
	}
}

void database::heap_del( index_t handler , index_t pos )
{
	table* table_0=tables[handler];
	swap_heap_entry( handler , pos , table_0->heap.size()-1 );
	table_0->heap.pop_back();
	heap_up( handler , pos );
	heap_down( handler , pos );
}

void database::heap_add( index_t handler , string key , index_t count_0 )
{
	table* table_0=tables[ handler ];
	table_0->heap.push_back( heap_entry( key,count_0 ) );
	table_0->hash_0[ key ] = table_0->heap.size();
	heap_up( handler , table_0->heap.size()-1 );
}

void database::heap_inc( index_t handler , string key , index_t delta )
{
	table* table_0=tables[ handler ];
	table_0->heap[ table_0->hash_0[ key ] ].count += delta;
}

string database::heap_min( index_t handler )
{
	return tables[ handler ]->heap[1];
}

/* hash */

template<class key_t , class value_t> 
hash<key_t,value_t>::hash( index_t size )
{
	vector<bool> prime( false , size+1 );
	for (int i=2; i*i<=size; i++)
	{
		if ( !prime[i] )
			for (int j=1;i*j<=size;j++)
				prime[i*j]=true;
	}
	for (int j=size; j>1; j--)
		if ( !prime[j] )
		{
			prime_0=j;
			break;
		}
	table=vector<hash_table_entry>( hash_table_entry() , size+1 );
	count=0;

}

template<class key_t , class value_t>
void hash<key_t,value_t>::add( key_t key , value_t value )
{
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
		if (!table[hash_value].used || !table[hash_value].valid || table[hash_value].key==key)
		{
			table[hash_value].used=true;
			table[hash_value].valid=true;
			table[hash_value].key=key;
			table[hash_value].value=value;
			count--;
			break;
		}
	}

}

template<class key_t , class value_t>
void hash<key_t,value_t>::del( key_t key )
{
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
		if (table[hash_value].used && table[hash_value].valid && table[hash_value].key==key)
		{
			table[hash_value].valid=false;
			count--;
		}
	}
}

template<class key_t , class value_t>
value_t& hash<key_t,value_t>::operator[](key_t key)
{
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
		if (!table[hash_value].used || table[hash_value].valid && table[hash_value].key==key)
			return table[hash_value].value;
	}
}

template<class key_t , class value_t>
bool hash<key_t,value_t>::find(key_t key)
{
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
		if (!table[hash_value].used)
			return false;
		else if ( table[hash_value].valid && table[hash_value].key==key)
			return true;
	}
	return false;
}

template<class key_t, class value_t>
index_t hash<key_t,value_t>::count()
{
	return count;
}

template<class key_t, class value_t>
index_t hash<key_t,value_t>::h0( index_t value )
{
	return value%prime_0;
}

template<class key_t, class value_t>
index_t hash<key_t,value_t>::h0( string value ) // BKDR hash
{
	index_t hash=0;
	index_t seed=131313; //strange hm......
	int l=value.length();
	for (int i=0;i<l;i++)
		hash = hash * seed + value[i];

	return hash % prime_0;
}

template<class key_t , class value_t>
index_t hash<key_t,value_t>::h( key_t key , index_t i )
{
	index_t key_1=h0(key);
	return (key_1 % prime_0 + i * key_1 % (prime_0-1) ) % prime_0;
}


