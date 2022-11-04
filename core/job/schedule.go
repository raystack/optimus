package job

type Schedule struct {
	startDate     string // to check
	endDate       string // to check
	interval      string // to check
	dependsOnPast bool
	catchUp       bool
	retry         *Retry
}

func (s Schedule) StartDate() string {
	return s.startDate
}

func (s Schedule) EndDate() string {
	return s.endDate
}

func (s Schedule) Interval() string {
	return s.interval
}

func (s Schedule) DependsOnPast() bool {
	return s.dependsOnPast
}

func (s Schedule) CatchUp() bool {
	return s.catchUp
}

func (s Schedule) Retry() *Retry {
	return s.retry
}

func NewSchedule(startDate string, endDate string, interval string, dependsOnPast bool, catchUp bool, retry *Retry) *Schedule {
	return &Schedule{startDate: startDate, endDate: endDate, interval: interval, dependsOnPast: dependsOnPast, catchUp: catchUp, retry: retry}
}

type Retry struct {
	count              int
	delay              int32
	exponentialBackoff bool
}

func (r Retry) Count() int {
	return r.count
}

func (r Retry) Delay() int32 {
	return r.delay
}

func (r Retry) ExponentialBackoff() bool {
	return r.exponentialBackoff
}

func NewRetry(count int, delay int32, exponentialBackoff bool) *Retry {
	return &Retry{count: count, delay: delay, exponentialBackoff: exponentialBackoff}
}
