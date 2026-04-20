#include "host/management/interface.h"
#include "host/environment/init.h"

int main()
{
    initConfig();

    run();

    freeData();

    return 0;
}
