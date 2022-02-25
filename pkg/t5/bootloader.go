package t5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/roffe/gocan/pkg/frame"
	"github.com/roffe/gocan/pkg/model"
	"github.com/roffe/gocan/pkg/srec"
)

var MyBooty = `S00E000004598B4E0A93E8A3FE93738F
S12350004EB8500C4EB8507A4EED0004207C00FFFA0410FC007F0810000367FA33FC040543
S123502000FFFA4833FC040500FFFA5033FC040500FFFA5433FC303000FFFA5233FC50309C
S123504000FFFA560279FFBF00FFFC140079001000FFFC1402798FDF00FFFC16007900505A
S123506000FFFC162A7C000400002C7C00FFFA263C3C55553E3CAAAA4E754EB851A8207C5C
S123508000005754227C0000575C101012C012FC000922FC0808080832BC08085B890C00DB
S12350A000C7670000CE0C00007F630000540C0000A5670000380C0000C0670000800C009F
S12350C000C2670000940C0000C36700009A0C0000C8670000B80C0000C9670000B84EB852
S12350E052100C3800C2575C66904E752018E188801821C0576611D0576A42116000FFE0CD
S1235100247C0000576B700642811218D5C11438576A14D85201B202670A51C8FFF642115E
S12351206000FFBC4EB854A812C06700FFB222840279FFBF00FFFC146000FFA44EB856BE59
S12351404EB8526012C06700FF9622820279FFBF00FFFC146000FF880279FFBF00FFFC14AA
S123516042116000FF7A421922BC0007FFFF6000FF6E2018E18880102440421912D212E22C
S123518012E212E212E212A26000FF544EB856346000FF4C4EB856BE421922CD12F8576464
S12351A012B857656000FF38207C00005754247C00F007FF267C00F0080040C1007C0700D3
S12351C014BC00024A1314BC00120813000067FA14BC001308130006660846C13C863C8740
S12351E060D814BC001316BC00087014741B1480520010D3B00263F614BC001208130000D2
S123520067FA14BC00130813000666D646C14E75227C0000575C247C00F007FF267C00F0A6
S1235220080040C1007C070014BC00024A1314BC0006421314BC00070813000067FA7009B8
S12352407410148052001699B00263F614BC000816BC008814BC000616BC008046C14E7502
S123526010385764123857650C00008966160C0100B8670001040C0100B4670000F470044F
S1235280600002240C000001661E0C010025670000E80C0100A7670000D80C0100206700EB
S12352A0018C7005600002000C000031660E0C0100B4670000BC7006600001EC0C00001F03
S12352C066120C0100D5674E0C01005D67507007600001D40C0000BF660E0C0100B5670086
S12352E0014C7008600001C00C000020660E0C010020670001387009600001AC0C0000378E
S1235300660E0C0100A467000124700A60000198700360000192243C000400006006243CD5
S1235320000200003B8701700000AAAA3B4655543BBC808001700000AAAA3B8701700000C7
S1235340AAAA3B4655543BBC101001700000AAAA323CABE04E7151C9FFFC50C03C863C878D
S1235360B03528FF6600013E538266F060000132243C000400006006243C0002000020026C
S1235380163C0040183C00C03ABCFFFF3ABCFFFF42554A3528FF67247A191B8328FF4235E4
S12353A028FF72154E7151C9FFFC1B8428FF720C4E7151C9FFFC4A3528FF660A3C863C87B4
S12353C0538266CC6004534566D042554A554A0566067002600000D0240050C0163C002057
S12353E0183C00A02A3C03E803E81B8328FF1B8328FF323C55F04E7151C9FFFC1B8428FFA2
S1235400720C4E7151C9FFFC3C863C87B03528FF66084845538266E46004534566CC4255C6
S12354204A554A456700007E6000007670011B8709300000AAAA1B86092055541BBC008015
S123544009300000AAAA1B8709300000AAAA1B86092055541BBC001009300000AAAA3C86E3
S12354603C871A35080008050007662A0805000567EC1A35080008050007661A1B8709303F
S12354800000AAAA1B86092055541BBC00F009300000AAAA600E53806700FF946000000250
S12354A04280600270014E75267C000057662853BBCC6200017C4282142B00041238576543
S12354C00C010020670000EE0C0100D567220C01005D671C0C0100B5670000DA0C010020BE
S12354E0670000D20C0100A4670000CA60000078200C0280000000FF99C0247C000057ECCC
S1235500223C0000010035B418FE18FE558166F6D5C0220215B3180418FF538166F695C0A8
S12355203B8701700000AAAA3B4655543BBCA0A001700000AAAA323C010039B218FE18FE34
S1235540558166F6323C55F04E7151C9FFFC323C0100363218FEB67418FE660000D4558151
S123556066F0600000C8163C0040183C00C0103328040C0000FF67247A19198328FF19800F
S123558028FF72154E7151C9FFFC198428FF720C4E7151C9FFFCB03428FF6606538266CEEF
S12355A06004530566D442554A554A05670000826000007A123C00A0200CD082C0BC0000C1
S12355C0000108400000163328040C0300FF67581B8709300000AAAA1B86092055541B81FE
S12355E009300000AAAA198328FFC63C0080183428FF1A04C83C0080B803672C0805000560
S123560067EC183428FFC83C0080B803671A1B8709300000AAAA1B86092055541BBC00F097
S123562009300000AAAA6008538266964240600270014E75247C0007FFFB4280428142829E
S123564042834284183C00FD12126700006C0C0100FF67000064102AFFFF95C1558AB0047B
S123566066E610321800040000300C40000A65025F00E98A8400530166E8C5430C0400FE81
S12356806704520460C2B682632E0C830007FFFF642652832442428042813C863C87121ACB
S12356A0D081121AD081B5C366F0B0B90007FFFC660642192280600412BC00014E75247CE0
S12356C0000057640079004000FFFC14323C55F04E7151C9FFFC50D550D51ABC009014D523
S12356E014AD000250D550D50C22008967640C120001675E0C12003167580279FFBF00FFED
S1235700FC14323CABE04E7151C9FFFC1B8701700000AAAA1B4655541BBC0090017000005F
S1235720AAAA323C55F04E7151C9FFFC14D514AD00021B8701700000AAAA1B4655541BBC96
S117574000F001700000AAAA323C55F04E7151C9FFFC4E7552
S10A5764000000000000003A
S10457EB00B9
S9035000AC`

func (t *Client) UploadBootLoader(ctx context.Context, callback model.ProgressCallback) error {
	start := time.Now()

	if callback != nil {
		callback(-float64(1884))
		callback("Uploading bootloader")
	}

	sr := srec.NewSrec()
	r := strings.NewReader(MyBooty)
	if err := sr.Parse(r); err != nil {
		return err
	}
	var progress float64 = 0

	for _, rec := range sr.Records {
		ccrc, err := rec.CalcChecksum()
		if err != nil {
			return fmt.Errorf("failed to calculate srec crc: %v", err)
		}
		if rec.Checksum != ccrc {
			return fmt.Errorf("srecord CRC: %X does not match calculated CRC: %X", rec.Checksum, ccrc)
		}

		switch rec.Srectype {
		case "S0":
			if err := t.sendBootloaderAddressCommand(ctx, 0, 0); err != nil {
				return err
			}
		case "S1":
			if err := t.sendBootloaderAddressCommand(ctx, rec.Address, rec.Length-3); err != nil {
				return err
			}

			r := bytes.NewReader(rec.Data)
			framecount := int(1 + (rec.Length-3)/7)
			seq := byte(0x00)
			for frameNo := 0; frameNo < framecount; frameNo++ {
				payload := []byte{seq, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
				for i := 1; i < 8; i++ {
					b, err := r.ReadByte()
					if err != nil {
						if err == io.EOF {
							break
						}
						return err
					}
					payload[i] = b
					progress++
				}

				resp, err := t.c.SendAndPoll(
					ctx,
					frame.New(0x5, payload, frame.ResponseRequired),
					150*time.Millisecond,
					0xC,
				)
				if err != nil {
					return err
				}
				data := resp.Data()
				if data[0] == 0x1C && data[1] == 0x01 && data[2] == 0x00 {
					if callback != nil {
						callback("Bootloader already running")
					}
					t.bootloaded = true
					return nil
				}

				if resp.Len() != 8 || data[0] != byte(frameNo*7) || data[1] != 0x00 {
					return fmt.Errorf("failed to upload bootloader: %X", data)
				}
				if callback != nil {
					callback(progress)
				}
				seq += 7
			}

		case "S9":
			if err := t.sendBootVectorAddressSRAM(rec.Address); err != nil {
				return fmt.Errorf("failed to sendBootVectorAddressSRAM")
			}
		}
	}
	//fmt.Printf("took: %s\n", time.Since(start).Round(time.Millisecond).String())
	if callback != nil {
		callback(fmt.Sprintf("Done, took: %s\n", time.Since(start).Round(time.Millisecond).String()))
	}
	t.bootloaded = true
	return nil
}

func (t *Client) sendBootloaderAddressCommand(ctx context.Context, address uint32, len byte) error {
	payload := []byte{0xA5, byte(address >> 24), byte(address >> 16), byte(address >> 8), byte(address), len, 0x00, 0x00}
	f, err := t.c.SendAndPoll(
		ctx,
		frame.New(0x5, payload, frame.ResponseRequired),
		250*time.Millisecond,
		0xC,
	)
	if err != nil {
		return fmt.Errorf("failed to sendBootloaderAddressCommand: %v", err)
	}
	data := f.Data()
	if f.Len() != 8 || data[0] != 0xA5 || data[1] != 0x00 {
		return fmt.Errorf("invalid response to sendBootloaderAddressCommand")
	}
	return nil
}

func (t *Client) sendBootVectorAddressSRAM(address uint32) error {
	data := []byte{0xC1, byte(address >> 24), byte(address >> 16), byte(address >> 8), byte(address), 0x00, 0x00, 0x00}
	return t.c.SendFrame(0x5, data, frame.Outgoing)
}

func (t *Client) sendBootloaderDataCommand(ctx context.Context, data []byte, length byte) error {
	frame := frame.New(0x5, data, frame.ResponseRequired)
	resp, err := t.c.SendAndPoll(ctx, frame, 150*time.Millisecond, 0xC)
	if err != nil {
		return fmt.Errorf("failed SBLDC: %v", err)
	}
	if resp.Data()[1] != 0x00 {
		return errors.New("failed to write")
	}
	return nil
}
