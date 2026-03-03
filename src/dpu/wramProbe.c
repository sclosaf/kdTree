#include <alloc.h>
#include <mram.h>
#include <defs.h>

static uint32_t measureWramAvailable()
{
    uint32_t total = 0;
    uint32_t minBlock = 1;
    uint32_t step = 1024;

    while(step >= minBlock)
    {
        volatile uint8_t arr[step];

        arr[0] = 0xAA;
        arr[step - 1] = 0xBB;

        if(arr[0] == 0xAA && arr[step - 1] == 0xBB)
        {
            total += step;
            step = step * 2;
        }
        else
            step /= 2;
    }

    return total;
}

__mram uint32_t result[1];

int main(void)
{
    result[0] = measureWramAvailable();
    return 0;
}
