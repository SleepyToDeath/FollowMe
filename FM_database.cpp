#include"FM_database.h"
#include<sstream>

using namespace FM_datatbase;
using std::string;
using std::vector;
using std::pair;
using std::stringstream;
using std::fstream;
using std::ios;
using std::endl;

index_t max( index_t a , index_t b )
{
	if ( a > b ) return a;
	else return b;
}

index_t atoi( string s )
{
	int l=s.length();
	index_t i=0;
	int j=0;
	while ( s[j]==' ' || s[j]=='\t' )
		j++;
	for (;j<l;j++)
		i=i*10+s[j]-'0';
	return i;
}

string itos( index_t i )
{
	stringstream ss;
	ss<<i;
	return ss.str();
}

string iexts( index_t i )
{
	int l = sizeof( index_t );
	string tmp="";
	for (int j=0; j<l; j++)
		tmp+=(char)(i>>(j*8));
	return tmp;
}

void put_int( fstream& fio , index_t i )
{
	string s = iexts( i );
	for (int i=0; i<sizeof(index_t); i++)
		fio.put( s[i] );
}

index_t get_int( fstream& fio )
{
	string s = "";
	for (int i=0; i<sizeof(index_t); i++)
		s+=fio.get();
	return atoi( s );
}

string get_string( fstream& fio , index_t len )
{
	string s = "";
	for (int i=0; i<len; i++)
		s += fio.get();
	return s;
}

bool operator > ( key a , key b )
{
	return ( a.key > b.key || a.key == b.key && a.index < b.index );
}

database::database( bool new_db )
{
	fstream fin;
	if ( new_db )
		fin.open( init_file_name.c_str() );
	else
		fin.open( ( store_directory+"database.meta" ).c_str() );
	string s;
	while (std::getline(fin,s))
	{
		if (s.find("cache_size")) 
		{
			size_t pos=s.find("=");
			db_meta_0.cache_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("cache_capacity")) 
		{
			size_t pos=s.find("=");
			db_meta_0.cache_capacity=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("block_size")) 
		{
			size_t pos=s.find("=");
			db_meta_0.block_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("recent_range")) 
		{
			size_t pos=s.find("=");
			db_meta_0.recent_range=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("table_num")) 
		{
			size_t pos=s.find("=");
			db_meta_0.table_num = atoi(s.substr(pos+1,s.length()-pos-1));
		}
	}
	if ( !new_db )
	{
		tables = vector<table*>( db_meta_0.table_num , nullptr );
		for ( int i=0; i<db_meta_0.table_num; i++)
			rebuild_table( i );
	}

}

database::~database()
{
	fstream fio;
	fio.open( (store_directory+"database.meta").c_str() );
	fio<<"cache_size="<<db_meta_0.cache_size<<endl;
	fio<<"cache_capacity="<<db_meta_0.cache_capacity<<endl;
	fio<<"block_size="<<db_meta_0.block_size<<endl;
	fio<<"recent_range="<<db_meta_0.recent_range<<endl;
	fio<<"table_num="<<db_meta_0.table_num<<endl;
	fio.close();

	for (int i=0; i<db_meta_0.table_num; i++)
	{
		clear_table( i );
		delete tables[i];
	}

}

void database::clear_table( index_t handler )
{
	table* tmpt = tables[ handler ];
	fstream fio( (store_directory+itos( handler )+".meta").c_str );
	fio<<"entry_size="<<tmpt->table_meta_0.entry_size<<endl;
	fio<<"value_size="<<tmpt->table_meta_0.value_size<<endl;
	fio<<"max_entry_num="<<tmpt->table_meta_0.max_entry_num<<endl;
	fio<<"free_head="<<tmpt->table_meta_0.free_head<<endl;
	fio<<"key_num="<<tmpt->table_meta_0.key_num<<endl;
	fio<<"central_key="<<tmpt->table_meta_0.central_key<<endl;
	fio<<"max_order="<<tmpt->table_meta_0.max_order<<endl;
	fio<<"key_len"<<endl;
	for( int i=0; i<tmpt->table_meta_0.key_num; i++)
		fio<<tmpt->table_meta_0.key_len[i]<<endl;

	fio.close();
	fio.open( (store_directory+itos( handler )+".dat").c_str );
	while ( tmpt->cache.count()>0 )
		write_back( handler );

}
/* table & cache */

void database::resume_table( index_t handler )
{
	table* tmpt = new table;
	tables[ handler ] = tmpt;
	tmpt->fio.open((store_directory+itos( handler )+".dat").c_str() , ios::binary | ios::in | ios::out );
	fstream fin( ( store_directory+itos( handler )+".meta" ).c_str() ); 
	string s;
	while (std::getline(fin,s))
	{
		if (s.find("entry_size")) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.entry_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("value_size")) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.value_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("max_entry_num")) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.max_entry_num=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("free_head")) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.free_head=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("key_num")) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.key_num=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("central_key")) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.central_key=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("max_order")) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.max_order=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("key_len"))
		{
			for (int i=0;i<tmpt->table_meta_0.key_num;i++)
				fin>>tmpt->table_meta_0.key_len[i];
			std::getline( fin , s );
		}
	}

	tmpt->cache = hash<index_t,cache_entry>( db_meta_0.cache_size );
	tmpt->heap = vector<heap_entry>( db_meta_0.cache_size , heap_entry() );
	tmpt->hash_0 = hash<std::string,std::pair<index_t,index_t> >( db_meta_0.cache_size );

	for (int i=0;i<tmpt->table_meta_0.key_num+1; i++)
		tmpt->keys.push_back( Btree( itos(handler)+"_"+itos(i) ) );

	ready = true;
}

index_t database::new_table( index_t len )
{
	table* tmpt=new table;
	tables.push_back(tmpt);
	tmpt->fio.open((store_directory+itos(tables.size()-1)+".dat").c_str() , ios::binary | ios::in | ios::out );
	tmpt->cache = hash<index_t,cache_entry>( db_meta_0.cache_size );
	tmpt->heap = vector<heap_entry>( db_meta_0.cache_size , heap_entry() );
	tmpt->hash_0 = hash<std::string,std::pair<index_t,index_t> >( db_meta_0.cache_size ); // key , <pos,head>

	tmpt->table_meta_0.value_size = len;
	tmpt->table_meta_0.max_entry_num = -1;
	tmpt->table_meta_0.free_head = -1;
	tmpt->table_meta_0.key_num = 0;
	tmpt->table_meta_0.central_key = -1;
	tmpt->table_meta_0.max_order = -1;
		

	ready = false;

	return tables.size-1;

}

index_t database::add_key( index_t handler , index_t len , bool central )
{
	table* tmpt = tables[ handler ];
	tmpt->table_meta_0.key_num++;
	tmpt->table_meta_0.key_len[ tmpt->table_meta_0.key_num-1 ] = len;
	if ( central )
		tmpt->table_meta_0.central_key = tmpt->table_meta_0.key_num-1;

	tmpt->keys.push_back( Btree( 
		itos( handler )+"_"+itos( tmpt->table_meta_0.key_num-1 ),
		db_meta_0.cache_size,
		db_meta_0.cache_capacity,
		db_meta_0.block_size,
		len	) );
}

index_t database::init_table( index_t handler )
{
	index_t size = 0;
	table_meta& tmpm = tables[ handler ]->table_meta_0;
	for (int i=0; i<tmpm.key_num; i++)
		size += tmpm.key_len[i];
	tmpm.entry_size = 1+2*sizeof( index_t )+tmpm.value_size+size;
	tmpm.key_len.push_back( sizeof( index_t );
	ready = true;
}

void database::write_back( index_t handler )
{
	table* table_0 = tables[ handler ];
	index_t i = table_0->hash_0[ heap_min( handler ) ].second;
	while ( i >= 0 )
	{
		cache_entry tmp_2 = table_0->cache[i];
		index_t tmp_i = write_data( handler , tmp_2.entry_0 );
		table_0->keys[ table_0->table_meta_0.key_num ].modify( iexts( tmp_2.entry_0.index ) , tmp_i );
		table_0->cache.del( i );
		i = tmp_2.next;
	}
	if ( table_0->heap[1].count <= 0 )
	{
		table_0->hash_0.del( heap_min( handler ) );
		heap_del( handler , 1 );
	}
	else
		table_0->hash_0[ heap_min( handler ) ].second = -1;
}
	
void database::add_to_cache( index_t handler , entry tmp )
{

	table* table_0 = tables[ handler ];

	cache_entry tmp_2;
	tmp_2.entry_0 = tmp;
	string tmp_ck = tmp.keys[ table_0->table_meta_0.central_key ];
	if ( table_0->hash_0.find( tmp_ck ) )
	{
		tmp_2.next = table_0->hash_0[ tmp_ck ].second;
		tmp_2.prev = -1;
		if ( tmp_2.next >= 0 )
			table_0->cache[ tmp_2.next ].prev = tmp_2.entry_0.index;
		table_0->hash_0[ tmp_ck ].second = tmp.index;
//		heap_inc( handler , tmp_ck , 1 );
	}
	else
	{
		table_0->hash_0.add( tmp_ck , pair<index_t,index_t>( -1 , tmp.index ) );
		heap_add( handler , tmp_ck , 1 );
		tmp_2.next = -1;
		tmp_2.prev = -1;
	}

	table_0->cache.add( tmp.index , tmp_2 );
	
}

void database::del_disk( index_t handler , index_t pos )
{
	table* tmpt = tables[ handler ];

	tmpt->fio.seekp( pos );
	tmpt->fio.put( (char)0 );
	put_int( tmpt->fio , tmpt->table_meta_0.free_head );
	tmpt->table_meta_0.free_head = pos;
}

void database::check_full( index_t handler )
{
	table* table_0 = tables[ handler ];

	if ( table_0->recent.size() > db_meta_0.recent_range )
	{
		heap_inc( handler , recent.front() , -1 );
		if ( table_0->heap[ table_0->hash_0[ recent.front() ].first ].count == 0 && table_0->hash_0[ recent.front() ].second == -1 )
		{
			heap_del( handler , hash_0[ recent.front() ].first );
			table_0->hash_0.del( recent.front() );
		}
		recent.pop();
	}

	while ( table_0->cache.count() >= db_meta_0.cache_capacity )
		write_back( handler );
}

index_t database::add( index_t handler , char* value , std::vector<std::string> keys )
{
	if ( !ready ) return -1;
	table* table_0=tables[ handler ];
	//prepare entry
	entry tmp;
	tmp.valid = 1;
	tmp.keys = keys;
	tmp.value = string( value );
	tmp.index = table_0->table_meta_0.max_order+1;
	table_0->table_meta_0.max_order++;
	tmp.keys.push_back( iexts(tmp.index) );

	//add to cache
/*	cache_entry tmp_2;
	tmp_2.entry_0 = tmp;
	string tmp_ck = tmp.keys[ table_0->table_meta_0.central_key ];
	if ( table_0->hash_0.find( tmp_ck ) )
	{
		tmp_2.next = table_0->hash_0[ tmp_ck ].second;
		tmp_2.prev = -1;
		if ( tmp_2.next >= 0 )
			table_0->cache[ tmp_2.next ].prev = tmp_2.entry_0.index;
		table_0->hash_0[ tmp_ck ].second = tmp.index;
	}
	else
	{
		table_0->hash_0.add( tmp_ck , pair<index_t,index_t>( -1 , tmp.index ) );
		heap_add( handler , tmp_ck , 1 );
		tmp_2.next = -1;
		tmp_2.prev = -1;
	}

	//update recent
	table_0->cache.add( tmp.index , tmp_2 ); */
	add_to_cache( handler , tmp );
	table_0->recent.push( tmp_ck );
	string tmp_ck = tmp.keys[ table_0->table_meta_0.central_key ];
	heap_inc( handler , tmp_ck , 1 );

	// add keys
	for (int i=0; i<table_0->table_meta_0.key_num+1; i++)
		table_0->keys[i].add( tmp.keys[i] , tmp.index );

	check_full( handler );

	return tmp.index;

/*	if ( recent.size() > db_meta_0.recent_range )
	{
		heap_inc( handler , recent.front() , -1 );
		if ( table_0->heap[ table_0->hash_0[ recent.front() ].first ].count == 0 && table_0->hash_0[ recent.front() ].second == -1 )
		{
			heap_del( handler , hash_0[ recent.front() ].first );
			table_0->hash_0.del( recent.front() );
		}
		recent.pop();
	}
*/

	// full , write back
/*	while ( table_0->cache.count() >= db_meta_0.cache_capacity )
	{
		index_t i = table_0->hash_0[ heap_min( handler ) ].second;
		while ( i >= 0 )
		{
			cache_entry tmp_2 = table_0->cache[i];
			index_t tmp_i = write_data( handler , tmp_2.entry_0 );

			table_0->keys[ table_0->table_meta_0.key_num ].modify( iexts( tmp_2.entry_0.index ) , tmp_i );
			table_0->cache.del( i );
			i = tmp_2.next;
		}
		if ( table_0->heap[1].count <= 0 )
		{
			table_0->hash_0.del( heap_min( handler ) );
			heap_del( handler , 1 );
		}
		else
			table_0->hash_0[ heap_min( handler ) ].second = -1;
	}*/

}

string database::del( index_t handler , index_t index )
{
	if ( !ready ) return "";
	table* tmpt = talbes[handler];
	entry tmpe;
	if ( tmpt->cache.find( index ) )
	{
		// remove from cache
		tmpe = tmpt->cache[ index ].entry_0;
		index_t tmpi = tmpt->cache[ index ].next;
		if ( tmpi >= 0 )
			tmpt->cache[ tmpi ].prev = tmpt->cache[ index ].prev;
		tmpi = tmpt->cache[ index ].prev;
		if ( tmpi >= 0 )
			tmpt->cache[ tmpi ].next = tmpt->cache[ index ].next;
		string tmpck = tmpt->cache[ index ].entry_0.keys[ tmpt->table_meta_0.central_key ];
		if ( tmpt->hash_0[ tmpck ].second == index )
			tmpt->hash_0[ tmpck ].second = tmpt->cache[ index ].next;
		tmpt->cache.del( index );
	}
	else
	{
		// remove from disk
		index_t tmpi = tmpt->keys[ tmpt->table_meta_0.key_num ].search( iexts( index ) );
		tmpe = read_data( tmpi );
		del_disk( handler , tmpi );
/*		tmpt->fio.seekp( tmpi );
		tmpt->fio.put( (char)0 );
		put_int( tmpt->fio , tmpt->table_meta_0.free_head );
		tmpt->table_meta_0.free_head = tmpi;*/
	}
		
	// remove keys
	for (int i=0; i<tmpt->table_meta_0.key_num+1; i++)
		tmpt->keys[i].del( tmpe.keys[i] , index );
	return tmpe.value;
}

string database::get( index_t handler , index_t index )
{
	if ( !ready ) return "";
	table* tmpt = tables[ handler ];
	entry wanted_entry;
	string tmpck;
	// read from disk
	if ( !tmpt->cache.find( index ) )
	{
		index_t pos_0 = tmpt->keys[ tmpt->table_meta_0.key_num ].search( iexts(index) );
		wanted_entry = read_data( handler , pos_0 );
		add_to_cache( handler , wanted_entry );
		tmpck = wanted_entry.keys[ tmpt->table_meta_0.central_key ];
		int count = 1;
		while ( count+pos_0/tmpt->table_meta_0.entry_size < max_entry_num && count*tmpt->table_meta_0.entry_size < block_size )
		{
			entry tmpe = read_data( -1 , false );
			if ( tmpe.valid && tmpe.keys[ tmpt->table_meta_0.central_key ] == tmpck )
			{
				count++;
				add_to_cache( handler , wanted_entry );
			}
			else
				break;
		}
		for (int i=0; i<count; i++)
			del_disk( handler , pos_0+i*tmpt->table_meta_0.entry_size );
	}
	else
	{
		wanted_entry = tmpt->cache[ index ].entry_0;
		tmpck = wanted_entry.keys[ tmpt->table_meta_0.central_key ];
	}
	// deal with heap , hash & recent affairs
	tmpt->recent.push( tmpck );
	heap_inc( handler , tmp_ck , 1 );

	check_full( handler );

	return wanted_entry.value;
}

index_t database::write_data( index_t handler , entry tmpe , bool relocate ) 
{
	table* tmpt = tables[ handler ];
	index_t tmpi = -1;
	if ( relocate )
	{
		tmpi = tmpt->table_meta_0.free_head;
		if ( tmpi >= 0 )
		{
			tmpt->fio.seekg( tmpi );
			tmpt->fio.get();
			tmpt->table_meta_0.free_head = get_int( tmpt->fio );
			tmpt->fio.seekp( tmpi );
		}
		else
		{
			tmpt->table_meta_0.max_entry_num++;
			tmpi = tmpt->table_meta_0.max_entry_num*tmpt->table_meta_0.entry_size;
		}
	}
	tmpt->fio.put( tmpe.valid );
	put_int( tmpt->fio , tmpe.index );
	put_int( tmpt->fio , tmpi );
	tmpt->fio<<tmpe.value;
	for (int i=0; i<tmpt->table_meta_0.key_num+1; i++)
		tmpt->fio<<tmpe.keys[i];
	return tmpi;
}

entry database::read_data( index_t handler , index_t pos , bool relocate )
{
	entry tmpe;
	fstream& fio = tables[ handler ]->fio;
	table_meta& tmpm = tables[ handler ]->table_meta_0;
	if ( relocate )
		fio.seekg( pos );
	fio.get( tmpe.valid );
	tmpe.index = get_int( fio );
	tmpe.pos = get_int( fio );
	tmpe.value = get_string( fio , tmpm.value_size );
	for (int i=0; i<tmpm.key_num+1; i++)
		tmpe.keys[i] = get_string( fio , tmpm.key_len[i] );
	return tmpe;

}
/* heap */

void database::swap_heap_entry( index_t handler , index_t i , index_t j )
{
	table* table_0 = tables[handler];
	table_0->hash_0[table_0->heap[i].key].first = j;
	table_0->hash_0[table_0->heap[j].key].first = i;
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
	table_0->hash_0[ key ].first = table_0->heap.size();
	heap_up( handler , table_0->heap.size()-1 );
}

void database::heap_inc( index_t handler , string key , index_t delta )
{
	table* table_0=tables[ handler ];
	table_0->heap[ table_0->hash_0[ key ].first ].count += delta;
}

string database::heap_min( index_t handler )
{
	return tables[ handler ]->heap[1].key;
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
	counter=0;

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
			counter--;
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
			counter--;
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
	return counter;
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
	return ( key_1 % prime_0 + i * key_1 % (prime_0-1) ) % prime_0;
}

Btree::carrier::carrier( std::string key_1_0 , std::string key_2_0 , index_t index_0 , Btree* tree_0)
:key_1(key_1_0),key_2(key_2_0),index(index_0),tree(tree_0)
{
	key_0 = key_1_0;
}

index_t Btree::carrier::next()
{
	index_t cur = tree->root;
	key tmp;
	tmp.index = index;
	tmp.key = key;
	index_t ans = -1;
	while ( cur >= 0 )
	{
		node& tmpn = tree->accessor( cur );
		index_t tmpi = tmpn.find( tmp );
		if ( tmpi < tmpn.key_num-1 )
			ans = tmpn.keys[ tmpi+1 ].index;
		cur = tmpn.sons[tmpi+1];
	}
	return ans;
}

index_t Btree::node::find( key key_0 )
{
	int i=0, j=key_num-1;
	while ( i<j )
	{
		k = (i+j)/2;
		if ( key_0 > keys[k] )
			i = k+1;
		else
			j = k;
	}
	return i-1;
}

Btree::Btree( std::string index_0 , index_t cache_size_0 , index_t cache_capacity_0 , index_t node_size_0 , index_t key_size_0 )
{
	meta.index = index_0;
	meta.cache_size = cache_size_0;
	meta.cache_capacity = cache_capacity_0;
	meta.node_size = node_size_0;
	meta.key_size = key_size_0;
	meta.root = -1;
	meta.max_pos = -1;
	meta.free_head = -1;
	meta.height = -1;
	meta.node_size = node_size/(meta.key_size+sizeof(index_t)*2);
	meta.node_size = max( meta.node_size , 3 );
	node_size_byte = node_size*meta.key_size+(node_size*2+3)*sizeof(index_t);
	cache = hash<index_t, node>( meta.cache_size );
	fstream fin( (index+".dat").c_str() , ios::binary | ios::in | ios::out );
}

Btree::Btree( std::string index_0 )
{
	fstream fin((store_directory+index+".meta").c_str());
	string s;
	while ( std::getline(fin,s) )
	{
		if (s.find("cache_size")) 
		{
			size_t pos = s.find("=");
			meta.cache_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("cache_capacity")) 
		{
			size_t pos = s.find("=");
			meta.cache_capacity = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("key_size")) 
		{
			size_t pos = s.find("=");
			meta.key_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("node_size")) 
		{
			size_t pos = s.find("=");
			meta.node_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("root")) 
		{
			size_t pos = s.find("=");
			meta.root = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("max_pos")) 
		{
			size_t pos = s.find("=");
			meta.max_pos = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("free_head")) 
		{
			size_t pos = s.find("=");
			meta.free_head = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("heght")) 
		{
			size_t pos = s.find("=");
			meta.height = atoi(s.substr(pos+1,s.length()-pos-1));
		}
	}

	node_size_byte = node_size*meta.key_size+(node_size*2+3)*sizeof(index_t);
	cache = hash<index_t, node>( meta.cache_size );
	fin.close();
	fstream fin( (index+".dat").c_str() , ios::binary | ios::in | ios::out );
}


Btree::~Btree()
{
	index_t tmpi = meta.max_pos/node_size_byte+1;
	for (int i=0; i<tmpi; i++)
		if ( cache.find( i ) )
			write_node( i , cache[i] );
	fio.close();
	fio( (meta.index+".meta").c_str );
	fio<<"cache_capacity="<<meta.cache_capacity<<endl;
	fio<<"cache_size="<<meta.cache_size<<endl;
	fio<<"node_size="<<meta.node_size<<endl;
	fio<<"key_size="<<meta.key_size<<endl;
	fio<<"max_pos="<<meta.max_pos<<endl;
	fio<<"free_head="<<meta.free_head<<endl;
	fio<<"root="<<meta.root<<endl;
	fio<<"height="<<meta.height<<endl;
	fio.close();
}

void Btree::add( string key_0 , index_t index_0 )
{
	key tmp;
	tmp.key = key;
	tmp.index = index_0;
	// make sure every key is of length key_size
	int l = tmp.key.length();
	for (int i=l; i<meta.key_size; i++)
		tmp.key+=" ";

	if ( root == -1 )
	{
		root = new_node();	
		height = 0;
	}
	index_t cur = root;
	for (int i=0; i<meta.height; i++)
	{
		node& tmpn = accessor( cur );
		int tmpi = tmpn.find( tmp );
		if ( tmpi>=0 && tmpn.keys[tmpi]==key_0 ) return;
		cur = tmpn.sons[tmpi+1];
	}

	node& tmpn = accessor( cur );
	int tmpi = tmpn.find( tmp );
	if ( tmpi>=0 && tmpn.keys[tmpi]==key_0 ) return;
	index_t new_son = -1;
	while ( cur>=0 )
	{
		node& tmpn = accessor( cur );
		int tmpi = tmpn.find( tmp );
		for (int i=tmpn.key_num-1; i>tmpi; i--)
		{
			tmpn.keys[i+1] = tmpn.keys[i];
			tmpn.sons[i+2] = tmpn.sons[i+1];
		}
		tmpn.keys[tmpi+1] = tmp;
		tmpn.sons[tmpi+2] = new_son;
		if ( tmpn.key_num < meta.key_size-1 ) break;
		tmpi = tmpn.key_num / 2;
		new_son = new_node();
		node& tmpn2 = accessor( new_son );
		for (int i=tmpi+1; i< tmpn.key_num; i++)
		{
			tmpn2.keys[i-tmpi-1] = tmpn.keys[i];
			tmpn2.sons[i-tmpi-1] = tmpn.sons[i];
		}
		tmpn2.sons[tmpn.key_num-tmpi] = tmpn.sons[tmpn.key_num];
		tmp = tmpn.keys[tmpi];
		tmpn.key_num = tmpi;
		tmpn2.key_num = key_num-tmpi-1;
		tmpn2.parent = tmpn.parent;
		cur = tmpn.parent;
	}
	if ( cur == -1 )
	{
		index_t tmpi = new_node();
		node& tmpn = accessor( tmpi );
		tmpn.keys[0] = tmp;
		tmpn.sons[0] = root;
		tmpn.sons[1] = new_son;
		accessor( root ).parent = tmpi;
		root = tmpi;
	}
		
}

/* possible problem: empty son not set to -1 */
void Btree::del( string key_0 , index_t index_0 )
{
	key tmp;
	tmp.key = key_0;
	tmp.index = index_0;
	index_t cur = root;
	int tmpi = -1;
	while ( cur>=0 )
	{
		node& tmpn = accessor( cur );
		tmpi = tmpn.find( tmp );
		if ( tmpn.keys[tmpi] == tmp ) break;
		cur = tmpn.sons[tmpi+1];
	}
	// now cur & tmpi is what I need
	if ( cur == -1 ) return;
	node& tmpn = accessor( cur );
	if ( tmpn.sons[tmpi+1] == -1 )
	{
		for (int i=tmpi; i<tmpn.key_num-1; i++)
			tmpn.keys[i] = tmpn.keys[i+1];
		tmpn.key_num--;
	}
	else
	{
		index_t tmpi2 = tmpn.sons[tmpi];
		index_t tmpi3;
		while ( tmpi2>=0 )
		{
			tmpi3 = tmpi2;
			node& tmpn2 = accessor( tmpi2 );
			tmpi2 = tmpn2.sons[tmpn2.key_num-1];
		}
		node& tmpn2 = accessor( tmpi2 );
		tmpn.keys[tmpi] = tmpn2.keys[tmpn2.key_num-1];
		tmpn2.key_num--;
		cur = tmpi2;
	}

	//now adjust cur
	while ( cur != root && accessor( cur ).key_num < (meta.node_size+1)/2-1 )
	{
		index_t tmpib;
		node& tmpn = accessor( cur );
		node& tmpnp = accessor( tmpn.parent );
		tmpi = tmpn.find( tmpn.keys[0] );
		if ( tmpi == -1 )
			tmpib = tmpnp.sons[1];
		else
			tmpib = tmpnp.sons[tmpi]
		node& tmpnb = accessor( tmpib );
		if ( tmpnb.key_num > (meta.node_size+1)/2-1 )
		{
			if ( tmpi >= 0 )
			{
				for (int i=tmpn.key_num; i>0; i--)
				{
					tmpn.keys[i]=tmpn.keys[i-1];
					tmpn.sons[i+1]=tmpn.sons[i];
				}
				tmpn.sons[1] = tmpn.sons[0];
				tmpn.keys[0] = tmpnp.keys[ tmpi ];
				tmpn.sons[0] = tmpnb.sons[ tmpnb.key_num ];
				tmpnp.keys[ tmpi ] = tmpnb.keys[ tmpnb.key_num-1 ];
				tmpnb.key_num--;
				tmpn.key_num++;
			}
			else
			{
				tmpn.keys[ tmpn.key_num ] = tmpnp.keys[0];
				tmpn.sons[ tmpn.key_num+1 ] = tmpnb.sons[0];
				tmpnp.keys[0] = tmpnb.keys[0];
				for (int i=0; i<tmpnb.key_num-1; i++)
				{
					tmpnb.keys[i]=tmpnb[i+1];
					tmpnb.sons[i]=tmpnb[i+1];
				}
				tmpnb.sons[ tmpnb.key_num-1 ] = tmpnb.sons[ tmpnb.key_num ];
				tmpn.key_num++;
				tmpnb.key_num++;
			}
		}
		else
		{
			
			if ( tmpi==-1 )
			{
				tmpn.keys[ tmpn.key_num ] = tmpnp.keys[ 0 ];
				for (int i=0; i<tmpnb.key_num; i++)
				 {
					 tmpn.keys[ i+tmpn.key_num+1 ] = tmpnb.keys[i];
					 tmpn.sons[ i+tmpn.key_num+1 ] = tmpnb.keys[i];
				 }
				 tmpn.sons[ tmpn.key_num+tmpnb.key_num+1 ] = tmpnb.sons[ tmpnb.key_num ];
				 tmpn.key_num = tmpn.key_num+tmpnb.key_num+1;
				 for (int i=0; i<tmpnp.key_num-1; i++)
				 {
					 tmpnp.keys[i] = tmpnp.keys[i+1];
					 tmpnp.sons[i+1] = tmpnp.sons[i+2];
				 }
				 tmpnp.sons[ tmpnp.key_num-1 ] = tmpnp.sons[ tmpnp.key_num ];
				 tmpnp.key_num--;
				 del_node( tmpib );
				 cur = tmpn.parent;
			}
			else
			{
				tmpnb.keys[ tmpnb.key_num ] = tmpnp.keys[ tmpi ];
				for (int i=0; i<tmpn.key_num; i++)
				 {
					 tmpnb.keys[ i+tmpnb.key_num+1 ] = tmpn.keys[i];
					 tmpnb.sons[ i+tmpnb.key_num+1 ] = tmpn.keys[i];
				 }
				// TODO
				 tmpnb.sons[ tmpn.key_num+tmpnb.key_num+1 ] = tmpn.sons[ tmpn.key_num ];
				 tmpnb.key_num = tmpn.key_num+tmpnb.key_num+1;
				 for (int i=tmpi; i<tmpnp.key_num-1; i++)
				 {
					 tmpnp.keys[i] = tmpnp.keys[i+1];
					 tmpnp.sons[i+1] = tmpnp.sons[i+2];
				 }
				 tmpnp.sons[ tmpnp.key_num-1 ] = tmpnp.sons[ tmpnp.key_num ];
				 tmpnp.key_num--;
				 del_node( cur );
				 cur = tmpnb.parent;
			}
		}
	}

	node& tmpn = accessor( cur );
	if ( cur == root && tmpn.key_num == 0 )
	{
		root = tmpn.sons[0];
		meta.height++;
	}
}

void Btree::modify( string key_0 , index_t new_value )
{
	pair<index_t,index_t> tmp = inner_search( key_0 );
	if ( tmp.first == -1 ) return;
	accessor( tmp.first ).keys[ tmp.second ].index = new_value;
}

carrier* Btree::search( std::string key_1 , std::string key_2 )
{
	pair<index_t,index_t> tmp = inner_search( key_1 );
	return new carrier(	key_1 , key_2 , accessor( tmp.first ).keys[ tmp.second ] , this );
}

index_t Btree::search( std::string key )
{
	pair<index_t,index_t> tmp = inner_search( key );
	if ( tmp.first == -1 ) return -1;
	return accessor( tmp.first ).keys[ tmp.second ].index;
}

index_t Btree::search( std::string key , index_t index )
{
	index_t cur = root;
	key tmp;
	tmp.index = index;
	tmp.key = key;
	while ( cur >= 0 )
	{
		node& tmpn = accessor( cur );
		index_t tmpi = tmpn.find( tmp );
		if ( tmpn.keys[tmpi] == tmp )
			return pair<index_t,index_t>( cur , tmpi );
		cur = tmpn.sons[tmpi+1];
	}
	return -1;
}

std::pair<index_t,index_t> Btree::inner_search( std::string key )
{
	index_t cur = root;
	key tmp;
	tmp.index = -1;
	tmp.key = key;
	while ( cur >= 0 )
	{
		node& tmpn = accessor( cur );
		index_t tmpi = tmpn.find( tmp );
		if ( tmpn.keys[tmpi].key == key )
			return pair<index_t,index_t>( cur , tmpi );
		cur = tmpn.sons[tmpi+1];
	}
	return pair<index_t,index_t>( -1 , -1 );
}

node& Btree::accessor( index_t pos )
{
	// read from data file
	if ( !cache.find( pos ) )
	{
		node tmp = read_node( pos )
		tmp.prev = -1;
		tmp.next = -1;
		cache.add( pos , tmp );
	}
	// update rank in cache
	if ( cache[pos].next >= 0 )
		cache[cache[pos].next].prev = cache[pos].prev;
	if ( cache[pos].prev >= 0 )
		cache[cache[pos].prev].next = cache[pos].next;
	if ( pos == cache_head )
		cache_head = cache[pos].next;
	if ( cache_head == -1 )
		cache_head = pos;
	cache[pos].prev = cache_tail;
	cache[pos].next = -1;
	if ( cache_tail >= 0 )
		cache[ cache_tail ].next = pos;
	cache_tail = pos;
	// eliminate oldest one
	if ( cache.count() > meta.cache_capacity )
	{
		index_t tmpi = cache_head;
		cache_head = cache[ cache_head ].next;
		write_node( tmpi , cache[ tmpi ] );
		cache.del( tmpi );
		cache[ cache_head ].prev = -1;
	}
		
	return cache[pos];
}

index_t Btree::new_node()
{
	node tmp;
	tmp.prev = -1;
	tmp.next = -1;	
	tmp.key_num = 0;
	tmp.parent = -1;
	key tmpk;
	tmpk.index = -1;
	tmpk.key = "";
	tmp.keys = vector<key>( meta.node_size+2 , tmpk );
	tmp.sons = vector<index_t>( meta.node_size+2 , -1 );
	if ( meta.free_head >= 0 )
	{
		index_t tmpi = meta.free_head;
		fio.seekg( meta.free_head );
		meta.free_head = get_int( fio );
		write_node( tmpi , tmp );
		return tmpi;
	}
	write_node( meta.max_pos , tmp );
	max_pos += node_size_byte;
	return max_pos - node_size_byte;
}

void Btree::del_node( index_t pos )
{
	if ( cache.find( pos )
	{
		if ( cache[pos].prev >= 0 )
			cache[ cache[pos].prev ].next = cache[pos].next;
		if ( cache[pos].next >= 0 )
			cache[ cache[pos].next ].prev = cache[pos].prev;
		if ( cache_head == pos )
			cache_head = cache[pos].next;
		if ( cache_tail == pos )
			cache_tail = cache[pos].prev;
	}
	fio.seekp( pos );
	put_int( fio , meta.free_head );
	meta.free_head = pos;
}

void Btree::write_node( index_t pos , node n )
{
	fio.seekp( pos );
	put_int( fio , n.key_num );
	put_int( fio , n.parent );
	for (int i=0; i<meta.node_size; i++)
	{
		fio<<n.keys[i].key;
		put_int( fio , n.keys[i].index );
	}
	for (int i=0; i<meta.node_size+1; i++)
		put_int( fio , n.sons[i] );
}

node Btree::read_node( index_t pos )
{
	fio.seekg( pos );
	node n;
	n.key_num = get_int( fio );
	n.parent = get_int( fio );
	for (int i=0; i<meta.node_size; i++)
	{
		string s = "";
		for (int i=0; i<meta.key_size(); i++)
			s+=fio.get();
		n.keys[i].key = s;
		n.keys[i].index = get_int( fio );
	}
	for (int i=0; i<meta.node_size+1; i++)
		n.sons[i] = get_int( fio );
	return n;
}











	

