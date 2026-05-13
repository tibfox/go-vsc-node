package devnet

import (
	"fmt"
	"log"
	"time"

	"github.com/vsc-eco/hivego"
)

// fundAccounts transfers TBD and TESTS from initminer to the witness
// accounts so they can pay contract deployment fees and have RCs.
func (d *Devnet) fundAccounts() error {
	hiveClient := hivego.NewHiveRpc([]string{d.DroneEndpoint()})
	hiveClient.ChainID = "18dcf0a285365fc58b71f18b3d3fec954aa0c141c44e4e5cb4cf777b9eab274e"
	wif := d.cfg.InitminerWIF

	var ops []hivego.HiveOperation
	for n := 1; n <= d.cfg.Nodes; n++ {
		witnessName := fmt.Sprintf("%s%d", d.cfg.WitnessPrefix, n)
		ops = append(ops,
			hivego.TransferOperation{
				From:   "initminer",
				To:     witnessName,
				Amount: "100.000 TBD",
				Memo:   "devnet funding",
			},
			hivego.TransferOperation{
				From:   "initminer",
				To:     witnessName,
				Amount: "10000.000 TESTS",
				Memo:   "devnet funding",
			},
		)
	}

	log.Printf("[devnet] funding %d witness accounts from initminer...", d.cfg.Nodes)
	_, err := hiveClient.Broadcast(ops, &wif)
	if err != nil {
		return fmt.Errorf("funding accounts: %w", err)
	}

	log.Printf("[devnet] accounts funded")
	return nil
}

// FundVSCBalance broadcasts a Hive transfer to vsc.gateway with a
// `to=<account>` memo, depositing `amountTBD` HBD into the named account's
// VSC ledger balance. Use this when a test needs a witness to make many
// contract calls — without VSC HBD, the witness only has the 10k-RC free
// tier, which the RC system caps for ~5 days (RC_RETURN_PERIOD), so a
// handful of map() calls is enough to exhaust the budget and leave
// subsequent calls failing with "gas_limit_hit / cost limit exceeded".
//
// amountTBD must be a 3-decimal Hive amount string, e.g. "100.000". The
// deposit credits in HBD-sat units (1 TBD = 1000 sat-HBD), and each
// sat-HBD becomes 1 RC after the free tier — so 100 TBD ≈ 100,000 RC,
// enough for 10+ map() calls before regen matters.
func (d *Devnet) FundVSCBalance(account, amountTBD string) error {
	hiveClient := hivego.NewHiveRpc([]string{d.DroneEndpoint()})
	hiveClient.ChainID = "18dcf0a285365fc58b71f18b3d3fec954aa0c141c44e4e5cb4cf777b9eab274e"
	wif := d.cfg.InitminerWIF

	op := hivego.TransferOperation{
		From:   "initminer",
		To:     "vsc.gateway",
		Amount: amountTBD + " TBD",
		Memo:   "to=" + account,
	}
	log.Printf("[devnet] funding VSC HBD ledger: %s TBD to %s", amountTBD, account)
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			log.Printf("[devnet] retrying VSC deposit (attempt %d)...", attempt+1)
			time.Sleep(3 * time.Second)
			hiveClient = hivego.NewHiveRpc([]string{d.DroneEndpoint()})
			hiveClient.ChainID = "18dcf0a285365fc58b71f18b3d3fec954aa0c141c44e4e5cb4cf777b9eab274e"
		}
		_, err := hiveClient.Broadcast([]hivego.HiveOperation{op}, &wif)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("vsc deposit for %s: %w", account, lastErr)
}
