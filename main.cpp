#include"FM_ui.h"
#include<iostream>
#include<fstream>
#include<ctime>
#include<cstdlib>
#include<string>
using namespace FM_database;
using std::string;
using std::vector;
using std::fstream;
using std::ofstream;
using std::endl;
using std::cout;

void basic()
{
	database db( true );
	index_t h0 = db.new_table( 5 );
	index_t k0 = db.add_key( h0 , 5 , true );
	db.init_table( h0 );

	std::string s1 = "12345";
	vector<string> k1;
	k1.push_back("s1k11");

	std::string s2 = "67890";
	vector<string> k2;
	k2.push_back("s2k21");

	index_t i1 = db.add( h0 , s1 , k1 );
	index_t i2 = db.add( h0 , s2 , k2 );

	Btree::carrier* c1 = db.search( h0 , k0 , k1[0] , k1[0] );
	Btree::carrier* c2 = db.search( h0 , k0 , k2[0] , k2[0] );
	std::cout<<"i1="<<i1<<" found one is "<<c1->next()<<std::endl;
	std::cout<<"i2="<<i2<<" found one is "<<c2->next()<<std::endl;
	cout<<db.get( h0 , i1 )<<endl;
	cout<<db.get( h0 , i2 )<<endl;

	cout<<"Basic is over"<<endl;
}

void small()
{
    int n = 100000;
    int k = 335;
    int len = 20;
    database* db = new database( true );
    index_t h1 = db->new_table( len );
    index_t k1 = db->add_key( h1 , len , true );
    db->init_table( h1 );
	vector<string> v1;
	vector< vector<string> > v2;
    for (int i=0; i<n; i++)
	{
        v1.push_back( mend_string(itos( i ),len) );
        v2.push_back( vector<string>( 1 , mend_string("k"+itos( i%(n/k) ),len) ) );
	}
	for (int i=0; i<n; i++)
        db->add( h1 , v1[i] , v2[i] );
    for (int i=0; i<n; i+=5)
        db->del( h1 , i );
	vector<Btree::carrier*> v3;
    for (int i=0; i<n/k; i++)
        v3.push_back( db->search( h1 , k1 , mend_string("k"+itos( i ),len) , mend_string("k"+itos( i ),len) ) );
    for (int i=0; i<n/k; i++)
	{
		cout<<'k'<<i<<" :"<<endl;
		index_t tmpi;
		tmpi = v3[i]->next();
		while (tmpi>=0)
		{
            cout<<db->get( h1 , tmpi )<<' ';
//            db->get( h1 , tmpi );
            tmpi = v3[i]->next();
		}
		cout<<endl;
	}
    delete db;
    cout<<" Now restart!\n";
    db= new database( false );
    v3.clear();
    for (int i=0; i<n/k; i++)
        v3.push_back( db->search( h1 , k1 , mend_string("k"+itos( i ),len) , mend_string("k"+itos( i ),len) ) );
    for (int i=0; i<n/k; i++)
    {
        cout<<'k'<<i<<" :"<<endl;
        index_t tmpi;
        tmpi = v3[i]->next();
        while (tmpi>=0)
        {
            cout<<db->get( h1 , tmpi )<<' ';
//            db->get( h1 , tmpi );
            tmpi = v3[i]->next();
        }
        cout<<endl;
    }


}

using std::clock_t;
clock_t recorder;

void initt()
{
    recorder = std::clock();
}

double recordt()
{
    return (double)(std::clock() - recorder)/CLOCKS_PER_SEC;
}

void benchmark()
{
    srand( time(NULL) );
    int n = 1000000;
    int k = 335;
    int len = 10;
    database* db = new database( true );
    index_t h1 = db->new_table( len );
    index_t k1 = db->add_key( h1 , len , true );
    db->init_table( h1 );
    vector<string> v1;
    vector< vector<string> > v2;
    for (int i=0; i<n; i++)
    {
        v1.push_back( mend_string(itos( i ),len) );
        v2.push_back( vector<string>( 1 , mend_string("k"+itos( rand()%(n/k) ),len) ) );
    }


    cout<<"Testing add\n";
    initt();
    for (int i=0; i<n; i++)
    {
        db->add( h1 , v1[i] , v2[i] );
        if ( (i+1) % 50000 == 0)
            cout<<i+1<<' '<<recordt()<<" s"<<endl;
    }


    vector<Btree::carrier*> v3;
    for (int i=0; i<n/k; i++)
        v3.push_back( db->search( h1 , k1 , mend_string("k"+itos( i ),len) , mend_string("k"+itos( i ),len) ) );


    cout<<"Testing get\n";
    int count = 0;
    initt();
    for (int i=0; i<n/k; i++)
    {
        index_t tmpi;
        tmpi = v3[i]->next();
        while (tmpi>=0)
        {
            count++;
            db->get( h1 , tmpi );
            tmpi = v3[i]->next();
            if ( count % 50000 == 0 )
                cout<<count+1<<' '<<recordt()<<" s"<<endl;
        }
    }


    cout<<"Testing del\n";
    initt();
    for (int i=0; i<n; i+=2)
    {
        db->del( h1 , i );
        if ( (i+2) % 50000 == 0 )
            cout<<(i+2)/2<<' '<<recordt()<<" s"<<endl;
    }


    cout<<"Testing correctness\n";
    v3.clear();
    for (int i=0; i<n/k; i++)
        v3.push_back( db->search( h1 , k1 , mend_string("k"+itos( i ),len) , mend_string("k"+itos( i ),len) ) );
    count = 0;
    vector<bool> vh( n , false );
    for (int i=0; i<n/k; i++)
    {
        index_t tmpi;
        tmpi = v3[i]->next();
        while (tmpi>=0)
        {
            count ++;
            string s = db->get( h1 , tmpi );
            vh[std::atoi( s.c_str() )] = true;
            tmpi = v3[i]->next();
            if ( count % 5000 == 0 )
                cout<< count/5000 <<" % finished\n";
        }
    }
    bool flag = true;
    for (int i=0; i<n; i++)
    {
        if ( i % 2 == 0 && vh[i] || i % 2 != 0 && !vh[i] )
        {
            flag = false;
            break;
        }
    }
    if (flag)
        cout<<" Pass! "<<endl;
    else
        cout<<" Fail...... "<<endl;


    cout<<" Storing \n";
    delete db;
    cout<<" Now restart!\n";
    db= new database( false );
    v3.clear();
    for (int i=0; i<n/k; i++)
        v3.push_back( db->search( h1 , k1 , mend_string("k"+itos( i ),len) , mend_string("k"+itos( i ),len) ) );
    for (int i=0; i<n/k; i++)
    {
//        cout<<'k'<<i<<" :"<<endl;
        index_t tmpi;
        tmpi = v3[i]->next();
        while (tmpi>=0)
        {
            db->get( h1 , tmpi );
//            db->get( h1 , tmpi );
            tmpi = v3[i]->next();
        }
//        cout<<endl;
    }
}


void normal_run( bool creative )
{
    if ( !creative )
    {
        ofstream fout( "first_run.txt" , std::ios::app );
        if ( fout.tellp() > 0 )
        {
            FM_ui ui( false );
            ui.run();
        }
        else
        {
            fout<<"false";
            FM_ui ui( true );
            ui.run();
        }
    }
    else
    {
        FM_ui ui( true );
        ui.run();
    }
}

int main( int argc , char** argv )
{
    if ( argc > 1  )
        normal_run( false );
    else
        benchmark();
}
