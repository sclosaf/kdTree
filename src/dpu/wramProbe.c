#include <alloc.h>
#include <mram.h>
#include <defs.h>

static uint32_t measureWramAvailable(void)
{
    uint32_t total = 0;
    uint32_t minBlock = 8;
    uint32_t step = 1024;
    void *ptr;

    while (step >= minBlock)
    {
        ptr = mem_alloc(step);
        if(ptr != NULL)
            total += step;
        else
            step /= 2;
    }

    mem_reset();
    return total;
}

__mram uint32_t result[1];

int main(void)
{
    result[0] = measureWramAvailable();
    return 0;
}
