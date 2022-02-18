package utils

func CloneMap(original map[string]interface{}) map[string]interface{} {
	clone := map[string]interface{}{}

	for key, val := range original {
		clone[key] = val
	}

	return clone
}

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
