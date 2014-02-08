#include"FM_cfg.h"


class FM_ui
{
	public:
	FM_ui();
	void run();
	void update(FM_cmd);

	private:
	void display();
	void init();
	void switch_page();
	void help();
	
	map<string,FM_page> pages;
	page_type cur_page;
};

