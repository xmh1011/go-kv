package utils

func RemoveString(slice []string, target string) []string {
	result := slice[:0]
	for _, s := range slice {
		if s != target {
			result = append(result, s)
		}
	}
	return result
}
