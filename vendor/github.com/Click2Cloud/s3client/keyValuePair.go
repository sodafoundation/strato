package s3client

//KeyValuePair KeyValuePair
type KeyValuePair struct {
	Key   string
	Value string
}

//KeyValuePairList KeyValuePairList
type KeyValuePairList []KeyValuePair

func (l KeyValuePairList) ToArray() []string {
	slice := make([]string, len(l))
	for i, v := range l {
		slice[i] = v.Value
	}
	return slice
}

func (l KeyValuePairList) Len() int {
	return len(l)
}

func (l KeyValuePairList) Less(i, j int) bool {
	return l[i].Key < l[j].Key
}

func (l KeyValuePairList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
