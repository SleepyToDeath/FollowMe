#include"FM_server.h"
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
using std::size_t;

index_t stoi( string s )
{
    index_t tmpi = 0;
    index_t mask = 0xff;
    for (size_t i=0; i<sizeof(index_t); i++)
    {
        tmpi = (tmpi<<8)+((index_t)s[i] & mask);
        mask<<8;
    }
    return tmpi;
}

string special_mend( string s )
{


}


string FM_user::value()
{
    return username
            + password
            + name
            + gender
            + birthday
            + home
            + phonenum;
}

vector<string> FM_user::keys()
{
    vector<string> keys;
    keys.push_back( username );
    keys.push_back( name );
    keys.push_back( birthday );
    keys.push_back( home );
    return keys;
}

FM_user::FM_user()
{
}

FM_user::FM_user( string value )
{
    size_t cur = 0;
    username = value.substr( cur , usernamel );
    cur += usernamel;
    password = value.substr( cur , passwordl );
    cur += passwordl;
    name = value.substr( cur , namel );
    cur += namel;
    gender = value.substr( cur , genderl );
    cur += genderl;
    birthday = value.substr( cur , birthdayl );
    cur += birthdayl;
    home = value.substr( cur ,homel );
    cur += homel;
    phonenum = value.substr( cur , phonenuml );
}

FM_news::FM_news()
{
}

vector<string> FM_news::keys()
{
    vector<string> keys;
    keys.push_back( iexts( publisher ) );
    return keys;
}

FM_news::FM_news( string value )
{
    size_t cur = 0;
    publisher = stoi( value.substr( cur , sizeof( index_t ) ) );
    cur += sizeof( index_t );
    source = stoi( value.substr( cur , sizeof( index_t ) ) );
    cur += sizeof( index_t );
    order = stoi( value.substr( cur , sizeof( index_t ) ) );
    cur += sizeof( index_t );
    news = value.substr( cur , news_len );
}

string FM_news::value()
{
    return iexts( publisher )
            + iexts( source )
            + iexts( order )
            + news;
}

string FM_relation::value()
{
    return iexts( following )
            + iexts( followed );
}

vector<string> FM_relation::keys()
{
    vector<string> keys;
    keys.push_back( iexts( following ) );
    keys.push_back( iexts( followed ) );
    return keys;
}

FM_relation::FM_relation()
{
}

FM_relation::FM_relation( string value )
{
    following = stoi( value.substr( 0 , sizeof( index_t ) ) );
    followed = stoi( value.substr( sizeof( index_t ) , sizeof( index_t ) ) );
}

FM_server::~FM_server()
{
    delete db;
    ofstream fout( "server.meta" );
    fout<<"userh="<<userh<<endl;
    fout<<"usernamekh="<<usernamekh<<endl;
    fout<<"namekh="<<namekh<<endl;
    fout<<"birthdaykh="<<birthdaykh<<endl;
    fout<<"homekh="<<homekh<<endl;
    fout<<"newsh="<<newsh<<endl;
    fout<<"publisherkh="<<publisherkh<<endl;
    fout<<"relationh="<<relationh<<endl;
    fout<<"followingkh="<<followingkh<<endl;
    fout<<"followedkh="<<followedkh<<endl;
    fout.close();
}

FM_server::FM_server( bool creative )
{
    if ( creative )
    {
        db = new database( true );

        userh = db->new_table( FM_user::len );
        usernamekh = db->add_key( userh , FM_user::usernamel , false );
        namekh = db->add_key( userh , FM_user::namel , true );
        birthdaykh = db->add_key( userh , FM_user::birthdayl , false );
        homekh = db->add_key( userh , FM_user::homel , false );
        db->init_table( userh );

        newsh = db->new_table( FM_news::len );
        publisherkh = db->add_key( newsh , sizeof( index_t ) , true );
        db->init_table( newsh );

        relationh = db->new_table( FM_relation::len );
        followingkh = db->add_key( relationh , sizeof( index_t ) , false );
        followedkh = db->add_key( relationh , sizeof( index_t ) , true );
        db->init_table( relationh );
    }
    else
    {
        db = new database( false );
        fstream fin;
        ofstream fout( "server.meta" , ios::app );
        fout.close();
        fin.open( "server.meta" );
        string s;
        while (std::getline(fin,s))
        {
            if (s.find("userh")!=string::npos)
            {
                size_t pos=s.find("=");
                userh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
            if (s.find("usernamekh")!=string::npos)
            {
                size_t pos=s.find("=");
                usernamekh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
            if (s.find("birthdaykh")!=string::npos)
            {
                size_t pos=s.find("=");
                birthdaykh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
            if (s.find("homekh")!=string::npos)
            {
                size_t pos=s.find("=");
                homekh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
            if (s.find("newsh")!=string::npos)
            {
                size_t pos=s.find("=");
                newsh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
            if (s.find("publisherkh")!=string::npos)
            {
                size_t pos=s.find("=");
                publisherkh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
            if (s.find("relationh")!=string::npos)
            {
                size_t pos=s.find("=");
                relationh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
            if (s.find("followingkh")!=string::npos)
            {
                size_t pos=s.find("=");
                followingkh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
            if (s.find("followedkh")!=string::npos)
            {
                size_t pos=s.find("=");
                followedkh=atoi(s.substr(pos+1,s.length()-pos-1));
            }
        }
    }
}

bool FM_server::signup( FM_user new_user )
{
    Btree::carrier* tmpc = db->search( userh , usernamekh , new_user.username , new_user.username );
    if ( tmpc->next() >= 0 )
        return false;
    delete tmpc;

    id = db->add( userh , new_user.value() , new_user.keys() );
    user = new_user;

    return true;

}

bool FM_server::signin( string username, string password )
{
    Btree::carrier* tmpc = db->search( userh , usernamekh , username , username );
    id = tmpc->next();
    delete tmpc;
    if ( id < 0 )
        return false;
    user = FM_user( db->get( userh , id ) );
    if ( user.password != password )
        return false;
    return true;
}

bool FM_server::modify( FM_user new_user ) // change info & password
{
    db->del( userh , id );
    db->add( userh , new_user.value() , new_user.keys() , id );
    return true;
}

bool FM_server::follow( index_t followed )
{
    Btree::carrier* tmpc = db->search( relationh , followingkh , iexts(id) , iexts(id) );
    index_t tmpi = tmpc->next();
    while ( tmpi >= 0 )
    {
        if ( FM_relation( db->get( relationh , tmpi ) ).followed == followed )
        {
            delete tmpc;
            return false;
        }
        tmpi = tmpc->next();
    }

    FM_relation tmpr;
    tmpr.following = id;
    tmpr.followed = followed;

    db->add( relationh , tmpr.value() , tmpr.keys() );

    return true;
}

bool FM_server::unfollow( index_t followed )
{
    Btree::carrier* tmpc = db->search( relationh , followingkh , iexts(id) , iexts(id) );
    index_t tmpi = tmpc->next();
    while ( tmpi >= 0 )
    {
        if ( FM_relation( db->get( relationh , tmpi ) ).followed == followed )
        {
            delete tmpc;
            db->del( relationh , tmpi );
            return true;
        }
        tmpi = tmpc->next();
    }
    return false;
}

std::vector<std::pair<long long, FM_user> > FM_server::search( string key , string key_1 , string key_2 )   // after that , each time this is called , an user is returned
{
    index_t tmph = -1;
    if ( key == "username")
        tmph = usernamekh;
    else if ( key == "name" )
        tmph = namekh;
    else if ( key == "birthday" )
        tmph = birthdaykh;
    else if ( homekh )
        tmph = homekh;
    Btree::carrier* tmpc = db->search( userh , tmph , key_1 , key_2 );
    index_t tmpi = tmpc->next();
    vector< pair<index_t,FM_user> > tmpv;
    while ( tmpi >= 0 )
    {
        tmpv.push_back( pair<index_t,FM_user>( tmpi , FM_user(db->get( userh , tmpi ) ) ) );
        tmpi = tmpc->next();
    }
    delete tmpc;
    return tmpv;
}

std::vector<std::pair<long long, FM_user> > FM_server::follow_list()     // the same as search
{
    Btree::carrier* tmpc = db->search( relationh , followingkh , iexts(id) , iexts(id) );
    index_t tmpi = tmpc->next();
    vector< pair<index_t,FM_user> > tmpv;
    while ( tmpi >= 0 )
    {
        index_t tmpid = FM_relation( db->get( relationh , tmpi ) ).followed;
        tmpv.push_back( pair<index_t,FM_user>( tmpid , FM_user(db->get( userh , tmpid ) ) ) );
        tmpi = tmpc->next();
    }
    delete tmpc;
    return tmpv;
}

bool FM_server::refresh()
{
    followeds.clear();
    news.clear();
    for (size_t i=0; i<carriers.size(); i++)
        delete carriers[i];
    carriers.clear();

    Btree::carrier* tmpc = db->search( relationh , followingkh , iexts(id) , iexts(id) );
    index_t tmpi = tmpc->next();
    while ( tmpi >= 0 )
    {
        followeds.push_back( FM_relation( db->get( relationh , tmpi ) ).followed );
        tmpi = tmpc->next();
    }
    for (size_t i=0; i<followeds.size(); i++)
    {
        carriers.push_back( db->search( newsh , publisherkh , iexts(followeds[i]) , iexts(followeds[i]) ) );
        index_t tmpi = carriers[i]->next();
        if ( tmpi < 0 )
            continue;
        news.push_back( FM_news( db->get( newsh , tmpi ) ) );
        size_t j = news.size()-1;
        while ( j>0 && news[j].order > news[j-1].order )
        {
            FM_news tmpn = news[j];
            news[j] = news[j-1];
            news[j-1] = tmpn;
            j--;
        }
    }
    cur_news = 0;
    return true;
}

FM_news FM_server::news_list()
{
    if ( cur_news==news.size() )
        return FM_news();

    index_t tmpid = -1;
    for (int i=0; i<followeds.size(); i++)
        if ( news[cur_news].publisher == followeds[i] )
        {
            tmpid = carriers[i]->next();
            break;
        }

    if ( tmpid >= 0 )
    {
        news.push_back( FM_news( db->get( newsh , tmpid ) ) );
        size_t j = news.size()-1;
        while ( j>0 && news[j].order > news[j-1].order )
        {
            FM_news tmpn = news[j];
            news[j] = news[j-1];
            news[j-1] = tmpn;
            j--;
        }
    }

    cur_news++;
    return news[cur_news-1];
}

bool FM_server::post( FM_news news )
{
    news.order = db->add( newsh , news.value() , news.keys() );
    db->del( newsh , news.order );
    db->add( newsh , news.value() , news.keys() , news.order );
    return true;
}

bool FM_server::share( index_t index )
{
    string tmps = db->get( newsh , index );
    if ( tmps == "" ) return false;
    FM_news tmpn = FM_news( tmps );
    tmpn.publisher = id;
    tmpn.order = db->add( newsh , tmpn.value() , tmpn.keys() );
    db->del( newsh , tmpn.order );
    db->add( newsh , tmpn.value() , tmpn.keys() , tmpn.order );
    return true;
}

FM_user FM_server::get_user()
{
    return user;
}

index_t FM_server::get_id()
{
    return id;
}

string FM_server::get_name( index_t id )
{
    string s = db->get( userh , id );
    if ( s == "" ) return "";
    else return FM_user( s ).name;
}
