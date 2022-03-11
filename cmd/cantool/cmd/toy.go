package cmd

import (
	"log"

	"github.com/roffe/gocan/pkg/ecu/t8"
	"github.com/roffe/gocan/pkg/gmlan"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(toyCmd)
}

var toyCmd = &cobra.Command{
	Use:    "toy",
	Short:  "toy",
	Hidden: true,
	Args:   cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		c, err := initCAN(ctx)
		if err != nil {
			return err
		}
		defer c.Close()

		tr := t8.New(c)
		//leg := legion.New(c)
		gm := gmlan.New(c)

		gm.TesterPresentNoResponseAllowed()

		//time.Sleep(50 * time.Millisecond)

		if err := gm.InitiateDiagnosticOperation(ctx, 0x02, 0x7E0, 0x7E8); err != nil {
			return err
		}

		if err := gm.DisableNormalCommunication(ctx, 0x7E0, 0x7E8); err != nil {
			return err
		}

		if err := gm.ReportProgrammedState(ctx, 0x7E0, 0x7E8); err != nil {
			return err
		}

		//if err := gm.ProgrammingModeRequest(ctx, 0x7E0, 0x7E8); err != nil {
		//	return err
		//}
		//
		//if err := gm.ProgrammingModeEnable(ctx, 0x7E0, 0x7E8); err != nil {
		//	return err
		//}

		gm.TesterPresentNoResponseAllowed()

		//log.Println("Requesting security access")

		if err := tr.RequestSecurityAccess(ctx); err != nil {
			return err
		}

		if err := tr.SetOilQuality(ctx, 37.30); err != nil {
			return err
		}

		gm.TesterPresentNoResponseAllowed()

		q, err := tr.GetOilQuality(ctx)
		if err != nil {
			return err
		}
		log.Printf("quality: %.2f", q)

		if err := tr.SetTopSpeed(ctx, 270); err != nil {
			return err
		}

		speed, err := tr.GetTopSpeed(ctx)
		if err != nil {
			return err
		}
		log.Println("speed: ", speed)

		if err := tr.SetRPMLimit(ctx, 1373); err != nil {
			return err
		}

		rpm, err := tr.GetRPMLimiter(ctx)
		if err != nil {
			return err
		}
		log.Println("rpm: ", rpm)

		vin, err := tr.GetVehicleVIN(ctx)
		if err != nil {
			return err
		}
		log.Println("VIN: ", vin)

		if err := gm.DeviceControl(ctx, 0x16, 0x7E0, 0x7E8); err != nil {
			return err
		}

		if err := gm.ReturnToNormalMode(ctx, 0x7E0, 0x7E8); err != nil {
			return err
		}

		return nil

		/*
			if err := leg.Bootstrap(ctx, infoCallback); err != nil {
				return err
			}

			b, err := leg.LegionIDemand(ctx, 0x02, 0x00)
			if err != nil {
				return err
			}
			log.Printf("%X", b)

			b2, err := leg.LegionIDemand(ctx, 0x01, 0x00)
			if err != nil {
				return err
			}

			log.Printf("%X", b2)

			if err := tr.ResetECU(ctx, nil); err != nil {
				return err
			}
			return nil
		*/
	},
}
