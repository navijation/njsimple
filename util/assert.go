package util

func AssertNoError(err error) {
	if err != nil {
		panic(err)
	}
}
