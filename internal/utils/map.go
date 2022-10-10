package utils

func MergeAnyMaps(maps ...map[string]interface{}) map[string]interface{} {
	imp := map[string]interface{}{}
	for _, mp := range maps {
		for k, v := range mp {
			imp[k] = v
		}
	}
	return imp
}

// MergeMaps can merge values from multiple maps into one
// It can also create clone of a map
func MergeMaps(maps ...map[string]string) map[string]string {
	smp := map[string]string{}
	for _, mp := range maps {
		for k, v := range mp {
			smp[k] = v
		}
	}
	return smp
}

func AppendToMap(gmap map[string]interface{}, mp map[string]string) {
	for k, v := range mp {
		gmap[k] = v
	}
}

func Contains[K comparable, V any](mp map[K]V, keys ...K) bool {
	for _, key := range keys {
		_, ok := mp[key]
		if !ok {
			return false
		}
	}
	return true
}
