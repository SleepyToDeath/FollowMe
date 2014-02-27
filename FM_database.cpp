#include"FM_database.h"
#include<sstream>
#include<iostream>

using namespace FM_database;
using std::string;
using std::vector;
using std::pair;
using std::stringstream;
using std::fstream;
using std::ofstream;
using std::ios;
using std::endl;
using std::cout;
using std::hex;

index_t FM_database::max( index_t a , index_t b )
{
	if ( a > b ) return a;
	else return b;
}

index_t FM_database::abs( index_t i )
{
	if ( i<0 ) return -i;
	else return i;
}

index_t FM_database::atoi( string s )
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

string FM_database::mend_string( string s , size_t len )
{
    size_t l = s.length();
    for (int i=l; i<len; i++)
        s+=' ';
    return s;
}

string FM_database::itos( index_t i )
{
	stringstream ss;
	ss<<i;
//    cout<<"int to string : "<<ss.str()<<endl;
	return ss.str();
}

string FM_database::iexts( index_t i )
{
	int l = sizeof( index_t );
	string tmp="";
	for (int j=0; j<l; j++)
        tmp=(char)((i>>(j*8)))+tmp;
	return tmp;
}

void FM_database::put_int( fstream& fio , index_t i )
{
	string s = iexts( i );
	for (size_t i=0; i<sizeof(index_t); i++)
		fio.put( s[i] );
}

index_t FM_database::get_int( fstream& fio )
{
	index_t tmpi = 0;
	index_t mask = 0xff;
	for (size_t i=0; i<sizeof(index_t); i++)
	{
		char ch;
		fio.get( ch );
//        cout<<"ch: "<<hex<<(int)ch<<endl;
		tmpi = (tmpi<<8)+((index_t)ch & mask);
		mask<<8;
	}
	return tmpi;
}

string FM_database::get_string( fstream& fio , index_t len )
{
	string s = "";
	for (int i=0; i<len; i++)
	{
		char ch;
		fio.get( ch );
		s+=ch;
	}
	return s;
}

bool FM_database::operator == ( key a , key b )
{
	return ( a.key == b.key && a.index == b.index );
}

bool FM_database::operator > ( key a , key b )
{
	return ( a.key > b.key || a.key == b.key && a.index < b.index );
}

database::database( bool new_db )
{
	fstream fin;
	if ( new_db )
		fin.open( init_file_name.c_str() );
	else
	{
        ofstream fout( ( store_directory+"database.meta" ).c_str() , ios::app );
        fout.close();
		fin.open( ( store_directory+"database.meta" ).c_str() );
	}
	string s;
	while (std::getline(fin,s))
	{
		if (s.find("cache_size")!=string::npos) 
		{
			size_t pos=s.find("=");
			db_meta_0.cache_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("cache_capacity")!=string::npos) 
		{
			size_t pos=s.find("=");
			db_meta_0.cache_capacity=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("block_size")!=string::npos) 
		{
			size_t pos=s.find("=");
			db_meta_0.block_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("recent_range")!=string::npos) 
		{
			size_t pos=s.find("=");
			db_meta_0.recent_range=atoi(s.substr(pos+1,s.length()-pos-1));
		}
        else if (s.find("table_num")!=string::npos)
        {
            size_t pos=s.find("=");
            db_meta_0.table_num=atoi(s.substr(pos+1,s.length()-pos-1));
        }
    }
	if ( !new_db )
	{
        tables = vector<table*>( db_meta_0.table_num , NULL );
		for ( int i=0; i<db_meta_0.table_num; i++)
			resume_table( i );
	}
    else
        db_meta_0.table_num = 0;

}

database::~database()
{
	fstream fio;
    ofstream fout( (store_directory+"database.meta").c_str(), ios::app );
	fout.close();
	fio.open( (store_directory+"database.meta").c_str() );
    if (! fio.is_open() ) cout<< " open fail 4 \n";
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
    ofstream fout( (store_directory+itos( handler )+".meta").c_str() , ios::app );
	fout.close();
	fstream fio( (store_directory+itos( handler )+".meta").c_str() );
    if (! fio.is_open() ) cout<< " open fail 8 \n";
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
    fout.open( (store_directory+itos( handler )+".dat").c_str() , ios::app );
	fout.close();
	fio.open( (store_directory+itos( handler )+".dat").c_str() );
    if (! fio.is_open() ) cout<< " open fail 5 \n";
//    while ( tmpt->cache.count()>0 )
//		write_back( handler );
    for (int i=0; i<=tmpt->table_meta_0.max_order; i++)
        if ( tmpt->cache.find( i ) )
        {
            index_t tmpi = write_data( handler , tmpt->cache[i].entry_0 );
            tmpt->keys[ tmpt->table_meta_0.key_num ]->modify( iexts( tmpt->cache[i].entry_0.index ) , tmpi );
            tmpt->cache.del( i );
        }
}
/* table & cache */

void database::resume_table( index_t handler )
{
	table* tmpt = new table;
	tables[ handler ] = tmpt;
    ofstream fout((store_directory+itos( handler )+".dat").c_str() , ios::app );
	fout.close();
	tmpt->fio.open((store_directory+itos( handler )+".dat").c_str() , ios::binary | ios::in | ios::out );
    if (! tmpt->fio.is_open() ) cout<< " open fail 6 \n";
    fstream fin( ( store_directory+itos( handler )+".meta" ).c_str() );
	string s;
	while (std::getline(fin,s))
	{
		if (s.find("entry_size")!=string::npos) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.entry_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("value_size")!=string::npos) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.value_size=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("max_entry_num")!=string::npos) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.max_entry_num=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("free_head")!=string::npos) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.free_head=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("key_num")!=string::npos) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.key_num=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("central_key")!=string::npos) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.central_key=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("max_order")!=string::npos) 
		{
			size_t pos=s.find("=");
			tmpt->table_meta_0.max_order=atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("key_len")!=string::npos)
		{
			for (int i=0;i<tmpt->table_meta_0.key_num;i++)
            {
                index_t tmpi;
                fin>>tmpi;
                tmpt->table_meta_0.key_len.push_back( tmpi );
            }
			std::getline( fin , s );
		}
	}
    tmpt->table_meta_0.key_len.push_back( sizeof(index_t) );

	tmpt->cache = hash<index_t,cache_entry>( db_meta_0.cache_size );
//	tmpt->heap = vector<heap_entry>( db_meta_0.cache_size , heap_entry() );
    tmpt->heap.push_back( heap_entry() );
    tmpt->hash_0 = hash<std::string,std::pair<index_t,index_t> >( db_meta_0.cache_size );

	for (int i=0;i<tmpt->table_meta_0.key_num+1; i++)
		tmpt->keys.push_back( new Btree( itos(handler)+"_"+itos(i) , true ) );

	tmpt->ready = true;
}

index_t database::new_table( index_t len )
{
	table* tmpt=new table;
	tables.push_back(tmpt);
    ofstream fout((store_directory+itos(tables.size()-1)+".dat").c_str() , ios::app );
	fout.close();
	tmpt->fio.open((store_directory+itos(tables.size()-1)+".dat").c_str() , ios::binary | ios::in | ios::out );
    if (! tmpt->fio.is_open() ) cout<< " open fail 7 \n";
    tmpt->cache = hash<index_t,cache_entry>( db_meta_0.cache_size );
//	tmpt->heap = vector<heap_entry>( db_meta_0.cache_size , heap_entry() );
    tmpt->heap.push_back( heap_entry() );
	tmpt->hash_0 = hash<std::string,std::pair<index_t,index_t> >( db_meta_0.cache_size ); // key , <pos,head>

	tmpt->table_meta_0.value_size = len;
	tmpt->table_meta_0.max_entry_num = -1;
	tmpt->table_meta_0.free_head = -1;
	tmpt->table_meta_0.key_num = 0;
	tmpt->table_meta_0.central_key = -1;
	tmpt->table_meta_0.max_order = -1;
		

	tmpt->ready = false;

	db_meta_0.table_num++;

	return tables.size()-1;

}

index_t database::add_key( index_t handler , index_t len , bool central )
{
	table* tmpt = tables[ handler ];
	tmpt->table_meta_0.key_num++;
	tmpt->table_meta_0.key_len.push_back(len);
	if ( central )
		tmpt->table_meta_0.central_key = tmpt->table_meta_0.key_num-1;

	tmpt->keys.push_back( new Btree( 
		itos( handler )+"_"+itos( tmpt->table_meta_0.key_num-1 ),
        db_meta_0.cache_size/(db_meta_0.block_size/len/10),
        db_meta_0.cache_capacity/(db_meta_0.block_size/len/10),
		db_meta_0.block_size,
		len	) );
	return tmpt->table_meta_0.key_num-1;
}

void database::init_table( index_t handler )
{
	index_t size = 0;
	table_meta& tmpm = tables[ handler ]->table_meta_0;
    tmpm.key_len.push_back( sizeof( index_t ) );
    for (int i=0; i<tmpm.key_num+1; i++)
		size += tmpm.key_len[i];
    tmpm.entry_size = 1+2*sizeof( index_t )+tmpm.value_size+size;

	table* tmpt = tables[ handler ];
	tmpt->keys.push_back( new Btree(
		itos( handler )+"_"+itos( tmpt->table_meta_0.key_num ),
        db_meta_0.cache_size/(db_meta_0.block_size/sizeof(index_t)/10),
        db_meta_0.cache_capacity/(db_meta_0.block_size/sizeof(index_t)/10),
		db_meta_0.block_size,
		sizeof( index_t ) ) );

	tables[ handler ]->ready = true;
}

void database::write_back( index_t handler )
{
	table* table_0 = tables[ handler ];
//	index_t i = table_0->hash_0[ heap_min( handler ) ].second; // changed
    index_t j = 1;
    while ( table_0->hash_0[ table_0->heap[j].key ].second < 0 )
        j++;
    index_t i = table_0->hash_0[ table_0->heap[j].key ].second;
    while ( i >= 0 )
	{
		cache_entry tmp_2 = table_0->cache[i];
		index_t tmp_i = write_data( handler , tmp_2.entry_0 );
        if ( tmp_2.entry_0.index == 15 )
        {
            cout<<' '<<endl;
        }
		table_0->keys[ table_0->table_meta_0.key_num ]->modify( iexts( tmp_2.entry_0.index ) , tmp_i );
		table_0->cache.del( i );
		i = tmp_2.next;
	}
    if ( table_0->heap[j].count <= 0 ) //changed
	{
//        string tmpk = heap_min( handler );//changed
        string tmpk = table_0->heap[j].key;
        heap_del( handler , j );
        table_0->hash_0.del( tmpk );
    }
	else
//		table_0->hash_0[ heap_min( handler ) ].second = -1; //changed
        table_0->hash_0[ table_0->heap[j].key ].second = -1;
}
	
void database::add_to_cache( index_t handler , entry tmp )
{
/*    if (tmp.index>1000)
    {
        cout<<" error may occur before add_to_cache \n";
    }*/

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
        heap_add( handler , tmp_ck , 0 );
		tmp_2.next = -1;
		tmp_2.prev = -1;
	}

	table_0->cache.add( tmp.index , tmp_2 );
	
}

void database::del_disk( index_t handler , index_t pos )
{
/*    if ( pos == 1330 || pos == 665 )
    {
        cout<<' ';
    }*/
	table* tmpt = tables[ handler ];

	tmpt->fio.seekp( pos );
	tmpt->fio.put( (char)0 );
	put_int( tmpt->fio , tmpt->table_meta_0.free_head );
	tmpt->table_meta_0.free_head = pos;
}

void database::check_full( index_t handler )
{
	table* table_0 = tables[ handler ];

	if ( (index_t)table_0->recent.size() > db_meta_0.recent_range )
	{
		heap_inc( handler , table_0->recent.front() , -1 );
		if ( table_0->heap[ table_0->hash_0[ table_0->recent.front() ].first ].count == 0 && table_0->hash_0[ table_0->recent.front() ].second == -1 )
		{
			heap_del( handler , table_0->hash_0[ table_0->recent.front() ].first );
			table_0->hash_0.del( table_0->recent.front() );
		}
		table_0->recent.pop();
	}

	while ( table_0->cache.count() >= db_meta_0.cache_capacity )
		write_back( handler );
}

Btree::carrier* database::search( index_t handler , index_t key_handler , std::string key_1_0 , std::string key_2_0 )
{
	return tables[ handler ]->keys[ key_handler ]->search( key_1_0 , key_2_0 );
}

index_t database::add( index_t handler , string value , std::vector<std::string> keys )
{
	table* table_0=tables[ handler ];
	if ( !table_0->ready ) return -1;
    //prepare entry

/*	int l = value.length();
	for (int i=l; i<table_0->table_meta_0.value_size; i++)
		value+=" "; 
	for (int i=0; i<table_0->table_meta_0.key_num; i++)
	{
		int l = keys[i].size();
		for ( int j=l; j<table_0->table_meta_0.key_len[i];j++)
			keys[i]+="";
    }*/
	entry tmp;
	tmp.valid = 1;
	tmp.keys = keys;
	tmp.value = value;
	tmp.index = table_0->table_meta_0.max_order+1;
	table_0->table_meta_0.max_order++;
	tmp.keys.push_back( iexts(tmp.index) );
/*    if (tmp.index== 19)
    {
        cout<<' ';
    }*/

	//add to cache
	add_to_cache( handler , tmp );
	string tmp_ck = tmp.keys[ table_0->table_meta_0.central_key ];
	table_0->recent.push( tmp_ck );
	heap_inc( handler , tmp_ck , 1 );

 /*   if (table_0->table_meta_0.max_order>=99780)
    {
        index_t pos_0 = table_0->keys[ table_0->table_meta_0.key_num ]->search( iexts(99780) );
        if (pos_0<0)
        {
            cout<<" error may occur here 8\n";
        }
    }*/

    // add keys
	for (int i=0; i<table_0->table_meta_0.key_num+1; i++)
		table_0->keys[i]->add( tmp.keys[i] , tmp.index );

/*    if (table_0->table_meta_0.max_order>=99780)
    {
        index_t pos_0 = table_0->keys[ table_0->table_meta_0.key_num ]->search( iexts(99780) );
        if (pos_0<0)
        {
            cout<<" error may occur here 8\n";
        }
    }*/

    // full , write back
    check_full( handler );

/*    if (table_0->table_meta_0.max_order>=0)
    {
        index_t tmpi1=table_0->keys[0]->count(-2);
        index_t tmpi2=table_0->keys[1]->count(-2);
        if (tmpi1!= tmpi2 || tmpi1!=table_0->table_meta_0.max_order+1)
        {
            cout<<" error may occur here 7\n";
        }
    }

    if (table_0->table_meta_0.max_order>=16)
    {
        index_t pos_0 = table_0->keys[ table_0->table_meta_0.key_num ]->search( iexts(15) );
        if (!table_0->cache.find(15) && pos_0==15 )
        {
            cout<<" error may occur here 8\n";
        }
    }*/

	return tmp.index;

}

string database::del( index_t handler , index_t index )
{
    table* tmpt = tables[handler];

/*    if ( index == 80 )
    {
        cout<<' ';
    }*/

/*    {
        index_t tmpi1=tmpt->keys[0]->count(-2);
        index_t tmpi2=tmpt->keys[1]->count(-2);
        if (tmpi1!= tmpi2 || tmpi1>tmpt->table_meta_0.max_order+1 || tmpi1+index/5!=5000)
        {
            cout<<" error may occur here 7\n";
        }
    }*/


	if ( !tmpt->ready ) return "";
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
		index_t tmpi = tmpt->keys[ tmpt->table_meta_0.key_num ]->search( iexts( index ) );
		tmpe = read_data( handler , tmpi );
		del_disk( handler , tmpi );
/*		tmpt->fio.seekp( tmpi );
		tmpt->fio.put( (char)0 );
		put_int( tmpt->fio , tmpt->table_meta_0.free_head );
		tmpt->table_meta_0.free_head = tmpi;*/
	}
		

    // remove keys
    for (int i=0; i<tmpt->table_meta_0.key_num; i++)
		tmpt->keys[i]->del( tmpe.keys[i] , index );


    index_t tmpp = tmpt->keys[ tmpt->table_meta_0.key_num ]->search( iexts( index ) );


    tmpt->keys[ tmpt->table_meta_0.key_num ]->del( iexts( index ) , tmpp );

/*    {
        index_t tmpi1=tmpt->keys[0]->count(-2);
        index_t tmpi2=tmpt->keys[1]->count(-2);
        if (tmpi1!= tmpi2 || tmpi1>tmpt->table_meta_0.max_order+1 || tmpi1+index/5!=4999)
        {
            cout<<" error may occur here 7\n";
        }
    }*/

    return tmpe.value;
}

string database::get( index_t handler , index_t index )
{
/*    if (index==90)
    {
        cout<<' ';
    }*/
	table* tmpt = tables[ handler ];
	if ( !tmpt->ready ) return "";
	entry wanted_entry;
	string tmpck;
	// read from disk
	if ( !tmpt->cache.find( index ) )
	{
		index_t pos_0 = tmpt->keys[ tmpt->table_meta_0.key_num ]->search( iexts(index) );
		wanted_entry = read_data( handler , pos_0 );
        if ( wanted_entry.index != index )
        {
            cout<<" error may occur here 3\n";
        }
		add_to_cache( handler , wanted_entry );
		tmpck = wanted_entry.keys[ tmpt->table_meta_0.central_key ];
		int count = 1;
		while ( count+pos_0/tmpt->table_meta_0.entry_size < tmpt->table_meta_0.max_entry_num && count*tmpt->table_meta_0.entry_size < db_meta_0.block_size )
		{
            entry tmpe = read_data( handler , -1 , false );
			if ( tmpe.valid && tmpe.keys[ tmpt->table_meta_0.central_key ] == tmpck )
			{
                count++;
//                cout<< tmpe.index << " added\n" <<endl;
                add_to_cache( handler , tmpe );
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
	heap_inc( handler , tmpck , 1 );

	check_full( handler );

	return wanted_entry.value;
}

index_t database::write_data( index_t handler , entry tmpe , bool relocate ) 
{
/*    if (tmpe.index==90)
    {
        cout<<' ';
    }*/
    table* tmpt = tables[ handler ];
/*    if ( tmpe.index>100 )
    {
        cout<< " error may occur here at write data \n";
    }*/
	index_t tmpi = -1;
	if ( relocate )
	{
		tmpi = tmpt->table_meta_0.free_head;
		if ( tmpi >= 0 )
		{
			tmpt->fio.seekg( tmpi );
			tmpt->fio.get();
			tmpt->table_meta_0.free_head = get_int( tmpt->fio );
            tmpt->fio.seekp( tmpi ); //cout<<" free area used\n";
		}
		else
		{
			tmpt->table_meta_0.max_entry_num++;
			tmpi = tmpt->table_meta_0.max_entry_num*tmpt->table_meta_0.entry_size;
            tmpt->fio.seekp( tmpi );
		}
	}
    while ( tmpt->fio.tellp() > tmpt->table_meta_0.max_entry_num*tmpt->table_meta_0.entry_size )
        tmpt->table_meta_0.max_entry_num++;

/*    {
        fstream fio( "0.dat" , ios::binary | ios::in | ios::out );
        fio.seekg( tmpi ); //cout<<"what the fuck!\n";
        char ch;
        fio.get( ch );
        if (ch==1)
        {
            cout<<"error may occur here 4\n";
        }
        fio.close();
    }*/
//    cout<< " fio position: "<< tmpt->fio.tellp()<<endl;
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
    tmpe.keys = vector<string>( tmpm.key_num+1 , "" );
    if ( relocate )
		fio.seekg( pos );
	fio.get( tmpe.valid );
	tmpe.index = get_int( fio );
	tmpe.pos = get_int( fio );
	tmpe.value = get_string( fio , tmpm.value_size );
	for (int i=0; i<tmpm.key_num+1; i++)
		tmpe.keys[i] = get_string( fio , tmpm.key_len[i] );
/*    if ( tmpe.index > 100 )
    {
        cout<<" error may occur at read data \n";
        cout<<" pos: "<<fio.tellg()<<endl;
    }*/
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
    table_0->hash_0[ key ].first = table_0->heap.size()-1;
	heap_up( handler , table_0->heap.size()-1 );
}

void database::heap_inc( index_t handler , string key , index_t delta )
{
	table* table_0=tables[ handler ];
    index_t tmpi = table_0->hash_0[ key ].first;
//    cout<<" heap_inc : tmpi = "<< tmpi<<endl;
    table_0->heap[ tmpi ].count += delta;
    heap_up( handler , tmpi );
    heap_down( handler , tmpi );
}

string database::heap_min( index_t handler )
{
	return tables[ handler ]->heap[1].key;
}

/* hash */

template<class key_t , class value_t> 
hash<key_t,value_t>::hash( index_t size )
{
	vector<bool> prime( size+1 , false );
	for (int i=2; i*i<=size; i++)
	{
		if ( !prime[i] )
			for (int j=1;i*j<=size;j++)
				prime[i*j]=true;
	}
    int j;
    for (j=size; j>1; j--)
		if ( !prime[j] )
		{
			prime_0=j;
			break;
		}
    j--;
    for (; j>1; j--)
        if ( !prime[j] )
        {
            prime_1=j;
            break;
        }
//	hash_table_entry tmphte;
	table=vector<hash_table_entry>( (size_t)size+1 );
//	for (size_t i=0; i<size+1; i++)
//		table.push_back( tmphte );
	counter=0;

}

template<class key_t , class value_t>
void hash<key_t,value_t>::add( key_t key , value_t value )
{
    bool flag = false;
//    cout<<endl;
	for (int i=0;i<prime_0;i++)
	{
		int hash_value=h(key,i);
//        cout<<" hash_value : "<<hash_value<<endl;
		if (!table[hash_value].used || !table[hash_value].valid || table[hash_value].key==key)
		{
			table[hash_value].used=true;
			table[hash_value].valid=true;
			table[hash_value].key=key;
			table[hash_value].value=value;
            counter++;
            flag = true;
			break;
		}
	}
    if ( !flag ) cout<<"hash add fail\n";

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
            break;
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
    cout<<" hash::[] may have error \n";
	return table[0].value;
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
    return value;
}

template<class key_t, class value_t>
index_t hash<key_t,value_t>::h0( string value ) // BKDR hash
{
	index_t hash=0;
	index_t seed=1313; //strange hm......
	int l=value.length();
	for (int i=0;i<l;i++)
		hash = hash * seed + value[i];

    return hash;
}

template<class key_t , class value_t>
index_t hash<key_t,value_t>::h( key_t key , index_t i )
{
    index_t key_1=h0(key);
//    cout<<key_1<<endl;
    return abs( key_1 % prime_0 + i *prime_1 ) % prime_0;
}

Btree::carrier::carrier( std::string key_1_0 , std::string key_2_0 , index_t index_0 , Btree* tree_0)
:key_1(key_1_0),key_2(key_2_0),index(index_0),tree(tree_0)
{
	key_0 = key_1_0;
//	initial = true;
//    if ( index_0>100 )
//    {
//        cout<<" error may occur here 2 \n";
//    }
}

index_t Btree::carrier::next()
{
	index_t cur = tree->meta.root;
	key tmp;
	tmp.index = index;
	tmp.key = key_0;
//	if ( !initial )
//	{
		while ( cur >= 0 )
		{
			node& tmpn = tree->accessor( cur );
			index_t tmpi = tmpn.find( tmp );
            if ( tmpi < tmpn.key_num-1 )
			{
                key_0 = tmpn.keys[ tmpi+1 ].key;
                index = tmpn.keys[ tmpi+1 ].index;
			}
			cur = tmpn.sons[tmpi+1];
		}
//	}
    if ( key_0 > key_2 || key_0 < key_1 || index == tmp.index )
    {
//        initial = false;
        return -1;
    }
//    initial = false;
    return index;
}

index_t Btree::node::find( key key_0 )
{
	int i=0, j=key_num;
	while ( i<j )
	{
		int k = (i+j)/2;
		if ( key_0 > keys[k] || key_0 == keys[k] )
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
	meta.node_size = meta.node_size/(meta.key_size+sizeof(index_t)*2);
	meta.node_size = max( meta.node_size , 3 );
	node_size_byte = meta.node_size*meta.key_size+(meta.node_size*2+3)*sizeof(index_t);
	cache = hash<index_t, node>( meta.cache_size );
    cache_tail = -1;
    cache_head = -1;
    ofstream fout((store_directory+meta.index+".dat").c_str() , ios::app );
	fout.close();
	fio.open( (store_directory+meta.index+".dat").c_str() , ios::binary | ios::in | ios::out );
    if (! fio.is_open() ) cout<< " open fail 1 \n";
//	cout<<"open state is :"<< fio.is_open()<<endl;
}

Btree::Btree( std::string index , bool resume )
{
	meta.index = index;
	fstream fin((store_directory+meta.index+".meta").c_str());
	string s;
	while ( std::getline(fin,s) )
	{
		if (s.find("cache_size")!=string::npos) 
		{
			size_t pos = s.find("=");
			meta.cache_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("cache_capacity")!=string::npos) 
		{
			size_t pos = s.find("=");
			meta.cache_capacity = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("key_size")!=string::npos) 
		{
			size_t pos = s.find("=");
			meta.key_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("node_size")!=string::npos) 
		{
			size_t pos = s.find("=");
			meta.node_size = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("root")!=string::npos) 
		{
			size_t pos = s.find("=");
			meta.root = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("max_pos")!=string::npos) 
		{
			size_t pos = s.find("=");
			meta.max_pos = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("free_head")!=string::npos) 
		{
			size_t pos = s.find("=");
			meta.free_head = atoi(s.substr(pos+1,s.length()-pos-1));
		}
		else if (s.find("heght")!=string::npos) 
		{
			size_t pos = s.find("=");
			meta.height = atoi(s.substr(pos+1,s.length()-pos-1));
		}
	}
    cache_tail = -1;
    cache_head = -1;

	node_size_byte = meta.node_size*meta.key_size+(meta.node_size*2+3)*sizeof(index_t);
	cache = hash<index_t, node>( meta.cache_size );
	fin.close();
    ofstream fout((store_directory+meta.index+".dat").c_str() , ios::app );
	fout.close();
	fio.open( (store_directory+meta.index+".dat").c_str() , ios::binary | ios::in | ios::out );
    if (! fio.is_open() ) cout<< " open fail 2 \n";
}


Btree::~Btree()
{
	index_t tmpi = meta.max_pos/node_size_byte+1;
	for (int i=0; i<tmpi; i++)
        if ( cache.find( i*node_size_byte ) )
            write_node( i*node_size_byte , cache[i*node_size_byte] );
	fio.close();
    ofstream fout( (store_directory+meta.index+".meta").c_str() , ios::app );
	fout.close();
    fio.open( (store_directory+meta.index+".meta").c_str() );
    if (! fio.is_open() ) cout<< " open fail 3 \n";
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

index_t Btree::count( index_t cur )
{
    if ( cur==-1 ) return 0;
    if ( cur==-2 ) cur = meta.root;
    node& tmpn = accessor( cur );
    index_t counter = tmpn.key_num;
    index_t l= tmpn.key_num+1;
    for (int i=0;i<l;i++)
    {
        node& tmpn = accessor( cur );
        counter+=count( tmpn.sons[i] );
    }
    return counter;
}


void Btree::add( string key_0 , index_t index_0 )
{
	key tmp;
	tmp.key = key_0;
	tmp.index = index_0;
/*    if ( tmp.index > 10000 )
    {
        cout<<" error may occur at Btree::add \n";
    }*/
	// make sure every key is of length key_size
/*	int l = tmp.key.length();
	for (int i=l; i<meta.key_size; i++)
        tmp.key+=" "; */
/*    if (index_0==15)
    {
        cout<<' ';
    }*/
/*    if (!fio.good())
    {
        cout<<" error may occur here a \n";
    }*/
/*    if (index_0>0)
        for (int i=0; i<=meta.max_pos/node_size_byte; i++)
        {
            node& tmpn=accessor(i*node_size_byte);
            if (tmpn.key_num==0 || tmpn.sons[0]>=0 && tmpn.sons[tmpn.key_num]<0)
            {
                cout<<" error may occur here 9 \n";
            }
        }

    if (!fio.good())
    {
        cout<<" error may occur here a \n";
    }*/
    if ( meta.root == -1 )
	{
		meta.root = new_node();	
		meta.height = 0;
	}
	index_t cur = meta.root;
	for (int i=0; i<meta.height; i++)
	{
		node& tmpn = accessor( cur );
		int tmpi = tmpn.find( tmp );
		if ( tmpi>=0 && tmpn.keys[tmpi]==tmp ) return;
		cur = tmpn.sons[tmpi+1];
	}

    node& tmpn = accessor( cur );
	int tmpi = tmpn.find( tmp );
	if ( tmpi>=0 && tmpn.keys[tmpi]==tmp ) return;
    index_t new_son = -1;


    while ( cur>=0 )
	{
/*        if (cur == 8208 && index_0 == 51595)
        {
            cout<<' ';
        }*/
		node& tmpn = accessor( cur );
		int tmpi = tmpn.find( tmp );
		for (int i=tmpn.key_num-1; i>tmpi; i--)
		{
			tmpn.keys[i+1] = tmpn.keys[i];
			tmpn.sons[i+2] = tmpn.sons[i+1];
		}
//        tmpn.sons[tmpi+2] = tmpn.sons[tmpi+1];
		tmpn.keys[tmpi+1] = tmp;
		tmpn.sons[tmpi+2] = new_son;
		tmpn.key_num++;
/*        for (int i=0;i<tmpn.key_num;i++)
        {
            if (tmpn.sons[0]>=0 && tmpn.sons[1]<0 || tmpn.keys[i].index>100 && tmpn.keys[i].index%35 !=0)
            {
                cout<<" error may occur here 1 \n";
            }
        }*/
        if ( tmpn.key_num < meta.node_size-1 ) break;
/*    if (cur == 8208)
    {
        cout<<" ";
    }*/
        tmpi = tmpn.key_num / 2;
        new_son = new_node();
        node& tmpn2 = accessor( new_son );
        for (int i=tmpi+1; i< tmpn.key_num; i++)
		{
			tmpn2.keys[i-tmpi-1] = tmpn.keys[i];
			tmpn2.sons[i-tmpi-1] = tmpn.sons[i];
		}
        tmpn2.sons[tmpn.key_num-tmpi-1] = tmpn.sons[tmpn.key_num];
		tmp = tmpn.keys[tmpi];
		tmpn2.key_num = tmpn.key_num-tmpi-1;
		tmpn.key_num = tmpi;
		tmpn2.parent = tmpn.parent;
        for (int i=0; i<tmpn2.key_num+1; i++)
        {
            node& tmpn2 = accessor( new_son );
            if ( tmpn2.sons[i]>=0 )
                accessor( tmpn2.sons[i] ).parent = new_son;
        }
        cur = accessor( cur ).parent;
	}

    if ( cur == -1 )
	{
		index_t tmpi = new_node();
		node& tmpn = accessor( tmpi );
		tmpn.keys[0] = tmp;
		tmpn.sons[0] = meta.root;
		tmpn.sons[1] = new_son;
        tmpn.key_num = 1;
		accessor( meta.root ).parent = tmpi;
        accessor( new_son ).parent = tmpi;
		meta.root = tmpi;
        meta.height ++;
	}
		
/*    if (!fio.good())
    {
        cout<<" error may occur here a \n";
    }*/
}

/* possible problem: empty son not set to -1 */
void Btree::del( string key_0 , index_t index_0 )
{
	key tmp;
	tmp.key = key_0;
	tmp.index = index_0;
/*	int l = tmp.key.length();
    for (int i=l; i<meta.key_size; i++)
        tmp.key+=" "; */
/*    if (index_0 == 70)
    {
        cout<<' ';
    }*/
	index_t cur = meta.root;
//    index_t prev = cur;
    int tmpi = -1;
	while ( cur>=0 )
	{
//        prev = cur;
        node& tmpn = accessor( cur );
		tmpi = tmpn.find( tmp );
        if ( tmpi>=0 && tmpn.keys[tmpi] == tmp ) break;
		cur = tmpn.sons[tmpi+1];
	}
//    cur = prev;
	// now cur & tmpi is what I need
    if ( cur == -1 ) return;
    node& tmpn = accessor( cur );
    // delete the key
	if ( tmpn.sons[tmpi+1] == -1 )
	{
        // leaf
		for (int i=tmpi; i<tmpn.key_num-1; i++)
			tmpn.keys[i] = tmpn.keys[i+1];
		tmpn.key_num--;
	}
	else
	{
        // non-leaf
        index_t tmpi3 = cur;
        index_t tmpi2 = tmpn.sons[tmpi];
		while ( tmpi2>=0 )
		{
			tmpi3 = tmpi2;
			node& tmpn2 = accessor( tmpi2 );
            tmpi2 = tmpn2.sons[tmpn2.key_num];
		}
        node& tmpn2 = accessor( tmpi3 );
        node& tmpn = accessor( cur );
        tmpn.keys[tmpi] = tmpn2.keys[tmpn2.key_num-1];
		tmpn2.key_num--;
        cur = tmpi3;
	}

	//now adjust cur
    //  complement or combination
	while ( cur != meta.root && accessor( cur ).key_num < (meta.node_size+1)/2-1 )
	{
		index_t tmpib;
		node& tmpn = accessor( cur );
		node& tmpnp = accessor( tmpn.parent );
        tmpi = tmpnp.find( tmpn.keys[0] );
		if ( tmpi == -1 )
            tmpib = tmpnp.sons[1]; // pick next brother
		else
            tmpib = tmpnp.sons[tmpi]; // pick previous brother
		node& tmpnb = accessor( tmpib );
		if ( tmpnb.key_num > (meta.node_size+1)/2-1 )
		{
            // complement
			if ( tmpi >= 0 )
			{
                // previous brother
				for (int i=tmpn.key_num; i>0; i--)
				{
					tmpn.keys[i]=tmpn.keys[i-1];
					tmpn.sons[i+1]=tmpn.sons[i];
				}
				tmpn.sons[1] = tmpn.sons[0];
				tmpn.keys[0] = tmpnp.keys[ tmpi ];
				tmpn.sons[0] = tmpnb.sons[ tmpnb.key_num ];
                if ( tmpn.sons[ 0 ] >= 0 )
                    accessor ( tmpn.sons[0] ).parent = cur;
				tmpnp.keys[ tmpi ] = tmpnb.keys[ tmpnb.key_num-1 ];
				tmpnb.key_num--;
				tmpn.key_num++;
			}
			else
			{
                // next brother
				tmpn.keys[ tmpn.key_num ] = tmpnp.keys[0];
				tmpn.sons[ tmpn.key_num+1 ] = tmpnb.sons[0];
                if ( tmpn.sons[ tmpn.key_num+1 ] >= 0 )
                    accessor ( tmpn.sons[ tmpn.key_num+1 ] ).parent = cur;
				tmpnp.keys[0] = tmpnb.keys[0];
				for (int i=0; i<tmpnb.key_num-1; i++)
				{
					tmpnb.keys[i]=tmpnb.keys[i+1];
					tmpnb.sons[i]=tmpnb.sons[i+1];
				}
                tmpnb.sons[ tmpnb.key_num-1 ] = tmpnb.sons[ tmpnb.key_num ];
				tmpn.key_num++;
                tmpnb.key_num--;
			}
		}
		else
		{
/*            if (cur==0)
            {
                cout<<' ';
            }*/
            // combination
			if ( tmpi==-1 )
			{
                // next brother
				tmpn.keys[ tmpn.key_num ] = tmpnp.keys[ 0 ];
				for (int i=0; i<tmpnb.key_num; i++)
				 {
                     tmpn = accessor ( cur );
                     tmpnb = accessor ( tmpib );
					 tmpn.keys[ i+tmpn.key_num+1 ] = tmpnb.keys[i];
					 tmpn.sons[ i+tmpn.key_num+1 ] = tmpnb.sons[i];
                     if ( tmpn.sons[ i+tmpn.key_num+1 ] >= 0 )
                         accessor( tmpn.sons[ i+tmpn.key_num+1 ] ).parent = cur;
				 }
				 tmpn.sons[ tmpn.key_num+tmpnb.key_num+1 ] = tmpnb.sons[ tmpnb.key_num ];
                 if ( tmpn.sons[ tmpn.key_num+tmpnb.key_num+1 ] >= 0 )
                     accessor( tmpn.sons[ tmpn.key_num+tmpnb.key_num+1 ] ).parent = cur;
                 tmpn.key_num = tmpn.key_num+tmpnb.key_num+1;
                 tmpnp = accessor( tmpn.parent );
				 for (int i=0; i<tmpnp.key_num-1; i++)
				 {
					 tmpnp.keys[i] = tmpnp.keys[i+1];
					 tmpnp.sons[i+1] = tmpnp.sons[i+2];
				 }
//				 tmpnp.sons[ tmpnp.key_num-1 ] = tmpnp.sons[ tmpnp.key_num ];
				 tmpnp.key_num--;
				 del_node( tmpib );
				 cur = tmpn.parent;
			}
			else
			{
                // previous brother
				tmpnb.keys[ tmpnb.key_num ] = tmpnp.keys[ tmpi ];
				for (int i=0; i<tmpn.key_num; i++)
				 {
                    tmpn = accessor ( cur );
                    tmpnb = accessor ( tmpib );
                     tmpnb.keys[ i+tmpnb.key_num+1 ] = tmpn.keys[i];
					 tmpnb.sons[ i+tmpnb.key_num+1 ] = tmpn.sons[i];
                     if ( tmpnb.sons[ i+tmpnb.key_num+1 ] >= 0 )
                         accessor( tmpnb.sons[ i+tmpnb.key_num+1 ] ).parent = tmpib;
                 }
				// TODO
				 tmpnb.sons[ tmpn.key_num+tmpnb.key_num+1 ] = tmpn.sons[ tmpn.key_num ];
                 if ( tmpnb.sons[ tmpn.key_num+tmpnb.key_num+1 ] >= 0 )
                     accessor( tmpnb.sons[ tmpn.key_num+tmpnb.key_num+1 ] ).parent = tmpib;
                 tmpnb.key_num = tmpn.key_num+tmpnb.key_num+1;
                 tmpnp = accessor( tmpn.parent );
                 for (int i=tmpi; i<tmpnp.key_num-1; i++)
				 {
					 tmpnp.keys[i] = tmpnp.keys[i+1];
					 tmpnp.sons[i+1] = tmpnp.sons[i+2];
				 }
//				 tmpnp.sons[ tmpnp.key_num-1 ] = tmpnp.sons[ tmpnp.key_num ]; //no need and is buggy
				 tmpnp.key_num--;
				 del_node( cur );
				 cur = tmpnb.parent;
			}
		}
	}

	node& tmpn2 = accessor( cur );
	if ( cur == meta.root && tmpn2.key_num == 0 )
	{
        // del root
		meta.root = tmpn2.sons[0];
        if ( meta.root>=0 )
        accessor( meta.root ).parent = -1;
        del_node( cur );
        meta.height--;
	}
}

void Btree::modify( string key_0 , index_t new_value )
{
/*	int l = key_0.length();
    for (int i=l; i<meta.key_size; i++)
        key_0+=" "; */
/*    if (key_0==iexts(15))
    {
        cout<<' ';
    }*/
    pair<index_t,index_t> tmp = inner_search( key_0 );
	if ( tmp.first == -1 ) return;
	accessor( tmp.first ).keys[ tmp.second ].index = new_value;
}

Btree::carrier* Btree::search( std::string key_1 , std::string key_2 )
{
/*	int l = key_1.length();
	for (int i=l; i<meta.key_size; i++)
        key_1+=" "; */
/*	l = key_2.length();
	for (int i=l; i<meta.key_size; i++)
        key_2+=" "; */
//    pair<index_t,index_t> tmp = inner_search( key_1 );
//    if (accessor( tmp.first ).keys[ tmp.second ].index>100)
//    {
//        cout<<" error may occur at Btree::search \n";
//    }
    return new carrier(	key_1 , key_2 , MAX_INDEX_T , this ); //accessor( tmp.first ).keys[ tmp.second ].index , this );
}

index_t Btree::search( std::string key )
{
/*	int l = key.length();
	for (int i=l; i<meta.key_size; i++)
        key+=" "; */
/*    if ( key == iexts(21) )
    {
        cout<<" ";
    }*/
	pair<index_t,index_t> tmp = inner_search( key );
	if ( tmp.first == -1 ) return -1;
	return accessor( tmp.first ).keys[ tmp.second ].index;
}

/*
index_t Btree::search( std::string key , index_t index )
{
	index_t cur = meta.root;
	key tmp;
	tmp.index = index;
	tmp.key = key;
	while ( cur >= 0 )
	{
		node& tmpn = accessor( cur );
		index_t tmpi = tmpn.find( tmp );
		if ( tmpn.keys[tmpi] == tmp )
			return ;
		cur = tmpn.sons[tmpi+1];
	}
	return -1;
}
*/

std::pair<index_t,index_t> Btree::inner_search( std::string key_0 )
{
/*	int l = key_0.length();
	for (int i=l; i<meta.key_size; i++)
        key_0+=" "; */
	index_t cur = meta.root;
	key tmp;
	tmp.index = -1;
	tmp.key = key_0;
	while ( cur >= 0 )
	{
		node& tmpn = accessor( cur );
		index_t tmpi = tmpn.find( tmp );
		if ( tmpi>=0 && tmpn.keys[tmpi].key == key_0 )
			return pair<index_t,index_t>( cur , tmpi );
		cur = tmpn.sons[tmpi+1];
	}
	return pair<index_t,index_t>( -1 , -1 );
}

Btree::node& Btree::accessor( index_t pos )
{
/*    if (pos<0 || pos > meta.max_pos )
    {
        cout<<"error may occur here b\n";
    }*/
 /*   {

        index_t tmpi = cache_tail;
        int count=0;
        cout<<"up count:"<<cache.count()<<" pos:"<<pos<<endl;
        while (tmpi!=cache_head)
        {
            cout<< tmpi << " -> ";
            count++;
            tmpi = cache[tmpi].prev;
        }
        cout<< tmpi<<endl;
        if (tmpi>=0) count++;
        if (count<cache.count())
        {
            cout<<" error may occur here 5\n";
        }
        tmpi = cache_head;
        count=0;
        while (tmpi!=cache_tail)
        {
            count++;
            tmpi = cache[tmpi].next;
        }
        if (tmpi>=0) count++;
        if (count<cache.count())
        {
            cout<<" error may occur here 5\n";
        }

    }*/


/*    if (pos == 32736)
    {
        cout<<' ';
    }
    if (!fio.good())
    {
        cout<<" error may occur here a \n";
    }*/
    // read from data file
	if ( !cache.find( pos ) )
	{
		node tmp = read_node( pos );
		tmp.prev = -1;
		tmp.next = -1;
		cache.add( pos , tmp );
	}
	// update rank in cache
	if ( cache[pos].next >= 0 )
		cache[cache[pos].next].prev = cache[pos].prev;
    if ( cache[pos].prev >= 0 && pos != cache_tail )
		cache[cache[pos].prev].next = cache[pos].next;
	if ( pos == cache_head )
		cache_head = cache[pos].next;
    if ( cache_head == -1 )
		cache_head = pos;
    if ( cache_tail != pos )
        cache[pos].prev = cache_tail;
//    else
//        cache[pos].prev = -1;
	cache[pos].next = -1;
    if ( cache_tail >= 0 && cache_tail != pos )
		cache[ cache_tail ].next = pos;
    cache_tail = pos;
	// eliminate oldest one
	if ( cache.count() > meta.cache_capacity )
	{
		index_t tmpi = cache_head;
        if ( cache[ cache_head ].next >= 0 )
            cache[ cache[ cache_head ].next ].prev = -1;
		cache_head = cache[ cache_head ].next;
		write_node( tmpi , cache[ tmpi ] );
		cache.del( tmpi );
//		cache[ cache_head ].prev = -1;
	}
/*        {
        index_t tmpi = cache_tail;
        int count=0;
        cout<<"down count:"<<cache.count()<<" pos:"<<pos<<endl;
        while (tmpi!=cache_head)
        {
            cout<< tmpi<<" -> ";
            count++;
            tmpi = cache[tmpi].prev;
        }
        cout<< tmpi<<endl;
        if (tmpi>=0) count++;
        if (count<cache.count())
        {
            cout<<" error may occur here 5\n";
        }
        tmpi = cache_head;
        count=0;
        while (tmpi!=cache_tail)
        {
            count++;
            tmpi = cache[tmpi].next;
        }
        if (tmpi>=0) count++;
        if (count<cache.count())
        {
            cout<<" error may occur here 5\n";
        }

    }*/

/*    if (cache[pos].sons[0]>=0 && cache[pos].sons[cache[pos].key_num]<0)
    {
        node& tmpn=cache[pos];
        cout<<" error may occur here 6\n";
    }*/
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
	tmpk.key = string( meta.key_size , ' ' );
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
	write_node( meta.max_pos+1 , tmp );
	meta.max_pos += node_size_byte;
	return meta.max_pos - node_size_byte + 1;
}

void Btree::del_node( index_t pos )
{
	if ( cache.find( pos ) )
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
    cache.del( pos );
	fio.seekp( pos );
	put_int( fio , meta.free_head );
	meta.free_head = pos;
}

void Btree::write_node( index_t pos , node n )
{
/*    if (pos==0)
    {
        cout<<' ';
    }*/
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

Btree::node Btree::read_node( index_t pos )
{
	fio.seekg( pos );
//    cout<<" fio state is:"<<fio.good()<<std::endl;
//    index_t tmpit= fio.tellg();
	node n;
	n.key_num = get_int( fio );
	n.parent = get_int( fio );
	n.keys = vector<key>( meta.node_size+2 );
	n.sons = vector<index_t>( meta.node_size+2 , -1 );
	for (int i=0; i<meta.node_size; i++)
	{
		string s = "";
		for (int i=0; i<meta.key_size; i++)
		{
			char ch;
			fio.get( ch );
			s+=ch;
		}
		n.keys[i].key = s;
		n.keys[i].index = get_int( fio );
	}
	for (int i=0; i<meta.node_size+1; i++)
		n.sons[i] = get_int( fio );
/*    for (int i=0; i<n.key_num; i++)
    {
        if (n.keys[i].index>10000)
        {
            cout<<" error may occur at read node \n";
        }
    }*/
	return n;
}











	

