#include"FM_ui.h"
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
using std::cin;
using std::hex;

FM_ui::FM_ui( bool creative )
{
    server = new FM_server( creative );
}

void FM_ui::run()
{
    cout<<"************Welcome To FollowMe!****************\n";
    while ( page > 0 )
    {
        switch( page )
        {
            case 1:
                page = signin();
                break;
            case 2:
                page = signup();
                break;
            case 3:
                page = modify();
                break;
            case 4:
                page = search();
                break;
            case 5:
                page = follow_list();
                break;
            case 6:
                page = news_list();
                break;
            case 7:
                page = publish();
                break;
            case 8:
                page = follow();
                break;
            case 9:
                page = share();
                break;
            default:
                page = 1;
        }
        cout<<endl;
    }
}

index_t read_int()
{
    index_t tmpi;
    string s;
    while (!(cin>>tmpi))
    {
        cout<<"An int please.\n";
        cin.clear();
        cin.sync();
        cin>>tmpi;
    }
    getline(cin,s);
    return tmpi;
}

string read_string( int n )
{
    string s;
    getline( cin , s );
    while ( s.length()>n )
    {
        cout<<"Too Long~~~~~~~~\n"
            <<"Again:";
        getline( cin , s );
    }
    return mend_string( s , n );
}


int FM_ui::signin()
{
    cout<<"\n"
        <<"Sign in and enjoy it! [0]\n"
        <<"\n"
        <<"New user? Sign up right now! [1]\n";
    if ( read_int() == 0 )
    {
        string username , password;
        cout<<" User name:\n";
        getline( cin , username );
        cout<<" password:\n";
        getline( cin , password );
        username = mend_string( username , FM_user::usernamel );
        password = mend_string( password , FM_user::passwordl );
        if ( server->signin( username , password ) )
        {
            return 6;
        }
        else
        {
            cout<<"Sorry. Sign in failed.\n";
            return 1;
        }
    }
    else
        return 2;
}

int FM_ui::signup()
{
    cout<<"----------------------------------------------\n";
    FM_user tmpu;
    cout<<"User name:\n";
    tmpu.username = read_string( tmpu.usernamel );
    cout<<"Password:\n";
    tmpu.password = read_string( tmpu.passwordl );
    cout<<"Your real name:\n";
    tmpu.name = read_string( tmpu.namel );
    cout<<"Your gender ( male[0] or female[1] ):\n";
    int sex = read_int();
    if ( sex == 0 )
        tmpu.gender = "male  ";
    else
        tmpu.gender = "female";
    cout<<"Birth year:\n";
    int year = read_int();
    tmpu.birthday = mend_string( itos( year ) , tmpu.birthdayl );
    cout<<"Where do you live?\n";
    tmpu.home = read_string( tmpu.homel );
    cout<<"Nearly finished! Please tell us your phone number:\n";
    tmpu.phonenum = read_string( tmpu.phonenuml );
    if ( server->signup( tmpu ) )
    {
        cout<<"Congratulations! You are a member of FollowMe now!\n";
        server->follow( server->get_id() );
        return 6;
    }
    else
    {
        cout<<"Sorry. That user name has been taken over by someone else.\n";
        return 2;
    }
}

int FM_ui::modify()
{
    FM_user tmpu = server->get_user();
    cout<<"Which info do you want to modify?\n"
        <<"password[0]  home[1]  phone number[2]\n";
    int choice = read_int();
    if ( choice == 0 )
    {
        cout<<"New password:\n";
        tmpu.password = read_string( tmpu.passwordl );
    }
    else if ( choice == 1 )
    {
        cout<<"Where do you move?\n";
        tmpu.home = read_string( tmpu.homel );
    }
    else
    {
        cout<<"Your new phone number:\n";
        tmpu.phonenum = read_string( tmpu.phonenuml );
    }
    server->modify( tmpu );
    cout<<"Your info has been updated.\n";
    return 6;
}

int FM_ui::search()
{
    cout<<"Search by :\n";
    cout<<"User name[0]  name[1]  birth year[2]  home[3]\n";
    int choice = read_int();
    vector< pair<index_t,FM_user> > list;
    if ( choice == 0 )
    {
        cout<<"His/Her user name:\n";
        string s = read_string( FM_user::usernamel );
        list = server->search( "username" , s , s );
    }
    else  if (choice == 1 )
    {
        cout<<"His/Her name:\n";
        string s = read_string( FM_user::namel );
        list = server->search( "name" , s , s );
    }
    else if (choice == 2 )
    {
        cout<<"Starting year:\n";
        int year1 = read_int();
        cout<<"Ending year:\n";
        int year2 = read_int();
        list = server->search( "birthday" ,
                               mend_string( itos(year1) , FM_user::birthdayl ) ,
                               mend_string( itos(year2) , FM_user::birthdayl ) );
    }
    else
    {
        cout<<"Where dose he/her live :\n";
        string s = read_string( FM_user::namel );
        list = server->search( "home" , s , s );
    }
    for (int i=0; i<list.size(); i++)
    {
        cout<<'['<<list[i].first<<"] "<<list[i].second.username<<endl;
        cout<<"name: "<<list[i].second.name<<" gender: "<<list[i].second.gender<<endl;
        cout<<endl;
    }
    return 8;
}

int FM_ui::follow_list()
{
    cout<<"You Followed These Friends \n";
    cout<<"\n";
    vector< pair<index_t,FM_user> > list = server->follow_list();
    for (int i=0; i<list.size(); i++)
    {
        cout<<'['<<list[i].first<<"] "<<list[i].second.username<<endl;
        cout<<"name: "<<list[i].second.name<<" gender: "<<list[i].second.gender<<endl;
        cout<<endl;
    }
    string s;
    getline( cin , s );
    return 6;
}

int FM_ui::news_list()
{
    server->refresh();
    bool over = false;
    while (true)
    {
        if ( !over )
            for (int i=0; i<30; i++)
            {
                FM_news tmpn = server->news_list();
                if ( tmpn.order < 0 )
                {
                    over = true;
                    break;
                }
                cout<<"["<< tmpn.order<<"] published by "<<server->get_name( tmpn.publisher )<<endl;
                if ( tmpn.publisher != tmpn.source )
                    cout<<"Originally published by "<<server->get_name( tmpn.source )<<endl;
                cout<<tmpn.news<<endl;
            }
        cout<<"You could:\n"
            <<"See next page         [0]    Share some news         [1]\n"
            <<"Put some news         [2]    View your following list[3]\n"
            <<"Search for new friends[4]    Modify your info        [5]\n"
            <<"Refresh               [6]    Logout                  [7]\n"
            <<"Quit                  [8]\n";
        int choice = read_int();
        if ( choice == 1 )
            return 9;
        else if ( choice == 2 )
            return 7;
        else if ( choice == 3 )
            return 5;
        else if ( choice == 4 )
            return 4;
        else if ( choice == 5 )
            return 3;
        else if ( choice == 6 )
            return 6;
        else if ( choice == 7 )
            return 1;
        else if ( choice == 8 )
            return 0;
    }
}

int FM_ui::publish()
{
    cout<<"Shout It Out Loudly :\n";
    string news = read_string( FM_news::news_len );
    FM_news tmpn;
    tmpn.value() = news;
    tmpn.source = server->get_id();
    tmpn.publisher = tmpn.source;
    server->post( tmpn );
    return 6;
}

int FM_ui::follow()
{
    cout<<"Want to follow another one?\n";
    cout<<"Yes[0]/No[1]\n";
    int choice = read_int();
    if ( choice == 0 )
    {
        cout<<"His/Her id:\n";
        index_t tmpid = read_int();
        server->follow( tmpid );
        return 8;
    }
    else
        return 6;
}

int FM_ui::share()
{
    cout<<"Want to share?\n";
    cout<<"Yes[0]/No[1]\n";
    int choice = read_int();
    if ( choice == 0 )
    {
        cout<<"News id:\n";
        index_t tmpid = read_int();
        server->share( tmpid );
        return 9;
    }
    else
        return 6;
}
