#include <alloc.h>
#include <mram.h>
#include <defs.h>

static uint32_t measureWramAvailable(void)
{
    uint32_t total = 0;
    uint32_t minBlock = 8;
    uint32_t step = 4096;
    void *ptr;

    while (step >= MIN_BLOCK)
    {
        ptr = mem_alloc(step);
        if(ptr != NULL)
        {
            total += step;
            mem_free(ptr);
        }
        else
        {
            step /= 2;
        }
    }

    return total;
}

__mram uint32_t result[1];

int main(void)
{
    result[0] = measureWramAvailable();
    return 0;
}
