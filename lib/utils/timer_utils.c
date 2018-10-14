#include "timer_utils.h"

#define MICROSSECONDS_IN_SECONDS 1000000

/** Local function that returns the ticks
 *	(number of microsseconds) since the Epoch.
 */
static suseconds_t get_ticks()
{
	struct timeval tmp;
	gettimeofday(&(tmp), NULL);

	return (tmp.tv_usec) + (tmp.tv_sec * MICROSSECONDS_IN_SECONDS);
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

suseconds_t timer_delta_us (stopwatch_t* t)
{
	if (t->running)
		return get_ticks() - (t->start_mark);

	if (t->paused)
		return t->pause_mark;

	// Will never actually get here
	return (t->pause_mark) - (t->start_mark);
}


long timer_delta_ms(stopwatch_t* t)
{
	return (timer_delta_us(t) / 1000);
}

long timer_delta_s(stopwatch_t* t)
{
	return (timer_delta_us(t) / 1000000);
}

long timer_delta_m(stopwatch_t* t)
{
	return (timer_delta_s(t) / 60);
}

long timer_delta_h(stopwatch_t* t)
{
	return (timer_delta_m(t) / 60);
}