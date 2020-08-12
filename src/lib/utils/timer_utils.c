#include "timer_utils.h"

#define MICROSSECONDS_IN_SECONDS 1000000
#define NANOSECONDS_IN_SECONDS 1000000000

/** Local function that returns the ticks
 *	(number of microsseconds) since the Epoch.
 */
static suseconds_t get_ticks()
{
	// struct timeval tmp;
	// gettimeofday(&(tmp), NULL);

	struct timespec tmp;
	clock_gettime(CLOCK_REALTIME, &tmp);

	return (tmp.tv_nsec) + (tmp.tv_sec * NANOSECONDS_IN_SECONDS);
}

void timer_start(stopwatch_t* t)
{
	t->start_mark = get_ticks();
	t->pause_mark = 0;
	t->running	  = true;
	t->paused	  = false;
}

void timer_pause(stopwatch_t* t)
{
	if (!(t->running) || (t->paused)) return;

	t->pause_mark = get_ticks() - (t->start_mark);
	t->running	  = false;
	t->paused	  = true;
}

void timer_unpause(stopwatch_t* t)
{
	if (t->running || !(t->paused)) return;

	t->start_mark = get_ticks() - (t->pause_mark);
	t->running	  = true;
	t->paused	  = false;
}

stw_nanosec_t timer_delta_ns(stopwatch_t *t){
	if (t->running)
		return get_ticks() - (t->start_mark);

	if (t->paused)
		return t->pause_mark;

	// Will never actually get here
	return (t->pause_mark) - (t->start_mark);
}

suseconds_t timer_delta_us (stopwatch_t* t)
{
	return (suseconds_t)(timer_delta_ns(t) / 1000); 
}


long timer_delta_ms(stopwatch_t* t)
{
	return (timer_delta_ns(t) / 1000000);
}

long timer_delta_s(stopwatch_t* t)
{
	return (timer_delta_ns(t) / 1000000000);
}

long timer_delta_m(stopwatch_t* t)
{
	return (timer_delta_s(t) / 60);
}

long timer_delta_h(stopwatch_t* t)
{
	return (timer_delta_m(t) / 60);
}
