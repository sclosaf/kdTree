#include "management/interface.h"
#include "environment/init.h"

int main()
{
    initConfig();
    run();
    freeData();
    return 0;
}
