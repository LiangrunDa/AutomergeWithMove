package opset

type LifeCycleList struct {
	trackingEvents []Event // sorted by OpId
	s              *OpSet
}

func NewLifeCycleList(id *OpIdWithValid, s *OpSet) *LifeCycleList {
	makeEvent := Event{true, id}
	trackingData := make([]Event, 0)
	trackingData = append(trackingData, makeEvent)
	return &LifeCycleList{
		trackingEvents: trackingData,
		s:              s,
	}
}

func (l *LifeCycleList) insert(event Event) {
	index, found := l.find(event.time)
	if found {
		panic("inserting an existing event")
	}
	l.trackingEvents = append(l.trackingEvents, event)
	if index+2 < len(l.trackingEvents) {
		copy(l.trackingEvents[index+2:], l.trackingEvents[index+1:])
		l.trackingEvents[index+1] = event
	}

}

func (l *LifeCycleList) find(time *OpIdWithValid) (int, bool) {
	// binary search
	left := 0
	right := len(l.trackingEvents) - 1
	for left <= right {
		mid := (left + right) / 2
		if l.trackingEvents[mid].time.Id == time.Id {
			return mid, true
		} else if l.trackingEvents[mid].time.Id.GreaterThan(l.s, &time.Id) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return right, false
}

// at least l.trackingEvents[0] is valid
func (l *LifeCycleList) isPresent(time *OpIdWithValid) bool {
	// binary search
	left := 0
	//left = l.ensureValid(left, 1)
	right := len(l.trackingEvents) - 1
	right = l.ensureValid(right, -1)

	for left <= right {
		mid := (left + right) / 2
		mid = l.ensureValid(mid, 1)
		// at least right/left is valid
		if l.trackingEvents[mid].time == time {
			return l.trackingEvents[mid].status
		} else if l.trackingEvents[mid].time.Id.GreaterThan(l.s, &time.Id) {
			right = mid - 1
			right = l.ensureValid(right, -1)
			// at least left is valid
		} else {
			left = mid + 1
			left = l.ensureValid(left, +1)
		}
	}
	return l.trackingEvents[right].status
}

func (l *LifeCycleList) ensureValid(index int, direction int) int {
	if index >= len(l.trackingEvents) || index < 0 {
		return index
	}
	v := l.isValid(l.trackingEvents[index].time)
	for !v {
		index += direction
		if index >= len(l.trackingEvents) || index < 0 {
			break
		}
		v = l.isValid(l.trackingEvents[index].time)
	}
	return index
}

func (l *LifeCycleList) isValid(id *OpIdWithValid) bool {
	return id.Valid
}

func (l *LifeCycleList) insertPresent(time *OpIdWithValid) {
	newEvent := Event{true, time}
	l.insert(newEvent)
}

func (l *LifeCycleList) insertTrash(time *OpIdWithValid) {
	newEvent := Event{false, time}
	l.insert(newEvent)
}

type Event struct {
	status bool // true for present, false for trash
	time   *OpIdWithValid
}
