package parsers

import (
	"testing"
)

func TestBalanceParser_Parse(t *testing.T) {

}

func TestBalanceParser_Address(t *testing.T) {

	data := "483045022100f4ece69a7c50c911b3af6fa017dcf22de4df66699cd85c5753634d85140b955602204996b677af3a0b5835b36ae1db6323a125f1525edd4727be3209a0535073f42201410412b80271b9e034006fd944ae4cdbdbc45ee30595c1f8961439385575f1973019b3ff615afed85a75737ff0d43cd81df74bc76004b45a6e7c9e2d115f364da1d7"

	addr := FakeAddr([]byte(data))
	t.Log(addr, len(addr))

}
