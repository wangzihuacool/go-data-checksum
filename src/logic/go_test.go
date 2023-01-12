package logic

import "fmt"

// 判断有序集slice1是否slice2的子集
func main() {
	subsetCheckFunc := func(subset []string, superset []string) bool {
		startIndex := 0
		for i := 0; i < len(subset); i++ {
			founded := false
			for j := startIndex; j < len(superset); j++ {
				if subset[i] == superset[j] {
					startIndex = j + 1
					founded = true
					break
				}
			}
			if founded == false {
				return false
			}
		}
		return true
	}

	slice1 := []string{"1", "2", "3"}
	slice2 := []string{"1", "2", "3", "4"}
	isSubset := subsetCheckFunc(slice1, slice2)
	fmt.Println("%v", isSubset)
}
