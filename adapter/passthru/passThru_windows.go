//go:build !arm64

package passthru

import (
	"bytes"
	"fmt"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows/registry"
)

type PassThru struct {
	dll                     *syscall.DLL
	passThruReadVersionProc *syscall.Proc
	passThruOpen            *syscall.Proc
	passThruClose           *syscall.Proc
	passThruConnect         *syscall.Proc
	passThruDisconnect      *syscall.Proc
	passThruReadMsgs        *syscall.Proc
	passThruWriteMsgs       *syscall.Proc
	passThruStartMsgFilter  *syscall.Proc
	passThruIoctl           *syscall.Proc
	passThruGetLastError    *syscall.Proc
}

func New(dllName string) (*PassThru, error) {
	dll, err := syscall.LoadDLL(dllName)
	if err != nil {
		return nil, err
	}
	passThruReadVersionProc, err := dll.FindProc("PassThruReadVersion")
	if err != nil {
		return nil, err
	}

	passThruOpen, err := dll.FindProc("PassThruOpen")
	if err != nil {
		return nil, err
	}

	passThruClose, err := dll.FindProc("PassThruClose")
	if err != nil {
		return nil, err
	}

	passThruConnect, err := dll.FindProc("PassThruConnect")
	if err != nil {
		return nil, err
	}
	passThruDisconnect, err := dll.FindProc("PassThruDisconnect")
	if err != nil {
		return nil, err
	}
	passThruReadMsgs, err := dll.FindProc("PassThruReadMsgs")
	if err != nil {
		return nil, err
	}
	passThruWriteMsgs, err := dll.FindProc("PassThruWriteMsgs")
	if err != nil {
		return nil, err
	}
	passThruStartMsgFilter, err := dll.FindProc("PassThruStartMsgFilter")
	if err != nil {
		return nil, err
	}
	passThruIoctl, err := dll.FindProc("PassThruIoctl")
	if err != nil {
		return nil, err
	}
	passThruGetLastError, err := dll.FindProc("PassThruGetLastError")
	if err != nil {
		return nil, err
	}

	return &PassThru{
		dll:                     dll,
		passThruReadVersionProc: passThruReadVersionProc,
		passThruOpen:            passThruOpen,
		passThruClose:           passThruClose,
		passThruConnect:         passThruConnect,
		passThruDisconnect:      passThruDisconnect,
		passThruReadMsgs:        passThruReadMsgs,
		passThruWriteMsgs:       passThruWriteMsgs,
		passThruStartMsgFilter:  passThruStartMsgFilter,
		passThruIoctl:           passThruIoctl,
		passThruGetLastError:    passThruGetLastError,
	}, nil
}

func (j *PassThru) Close() error {
	return j.dll.Release()
}

func (j *PassThru) PassThruConnect(deviceID, protocolID, flags, baudRate uint32, pChannelID *uint32) error {
	// long PassThruConnect(unsigned long DeviceID, unsigned long ProtocolID, unsigned long Flags, unsigned long BaudRate, unsigned long *pChannelID);
	ret, _, _ := j.passThruConnect.Call(
		uintptr(deviceID),
		uintptr(protocolID),
		uintptr(flags),
		uintptr(baudRate),
		uintptr(unsafe.Pointer(pChannelID)),
	)
	return CheckError(uint32(ret))
}

func (j *PassThru) PassThruDisconnect(channelID uint32) error {
	// long PassThruDisconnect(unsigned long ChannelID);
	ret, _, _ := j.passThruDisconnect.Call(
		uintptr(channelID),
	)
	return CheckError(uint32(ret))
}

func (j *PassThru) PassThruClose(deviceID uint32) error {
	// long PassThruClose(unsigned long DeviceID);
	ret, _, _ := j.passThruClose.Call(
		uintptr(deviceID),
	)
	return CheckError(uint32(ret))
}

func (j *PassThru) PassThruOpen(deviceName string, pDeviceID *uint32) error {
	var pName *string
	if deviceName != "" {
		pName = &deviceName
	}
	// long PassThruOpen(void* pName, unsigned long *pDeviceID);
	ret, _, _ := j.passThruOpen.Call(
		uintptr(unsafe.Pointer(pName)),
		uintptr(unsafe.Pointer(pDeviceID)),
	)
	return CheckError(uint32(ret))
}

func (j *PassThru) PassThruReadMsg(channelID uint32, pMsg *PassThruMsg, timeout uint32) (uint32, error) {
	pNumMsgs := uint32(1)
	// long PassThruReadMsgs(unsigned long ChannelID, PassThruMsg *pMsg, unsigned long *pNumMsgs, unsigned long Timeout);
	ret, _, _ := j.passThruReadMsgs.Call(
		uintptr(channelID),
		uintptr(unsafe.Pointer(pMsg)),
		uintptr(unsafe.Pointer(&pNumMsgs)),
		uintptr(timeout),
	)
	if err := CheckError(uint32(ret)); err != nil {
		if str, err2 := j.PassThruGetLastError(); err2 == nil {
			return 0, fmt.Errorf("%s: %w", str, err)
		} else {
			return 0, err
		}
	}
	return pNumMsgs, nil
}

func (j *PassThru) PassThruReadMsgs(channelID uint32, pMsg []*PassThruMsg, pNumMsgs *uint32, timeout uint32) error {
	// long PassThruReadMsgs(unsigned long ChannelID, PassThruMsg *pMsg, unsigned long *pNumMsgs, unsigned long Timeout);
	ret, _, _ := j.passThruReadMsgs.Call(
		uintptr(channelID),
		uintptr(unsafe.Pointer(&pMsg)),
		uintptr(unsafe.Pointer(pNumMsgs)),
		uintptr(timeout),
	)
	if err := CheckError(uint32(ret)); err != nil {
		if str, err2 := j.PassThruGetLastError(); err2 == nil {
			return fmt.Errorf("%s: %w", str, err)
		} else {
			return err
		}
	}
	return nil
}

func (j *PassThru) PassThruReadMsgs2(channelID uint32, numMsgs *uint32, timeout uint32) (int, []PassThruMsg, error) {
	// long PassThruReadMsgs(unsigned long ChannelID, PassThruMsg *pMsg, unsigned long *pNumMsgs, unsigned long Timeout);
	rMsgs := make([]PassThruMsg, *numMsgs)

	ret, _, _ := j.passThruReadMsgs.Call(
		uintptr(channelID),
		uintptr(unsafe.Pointer(&rMsgs)),
		uintptr(unsafe.Pointer(numMsgs)),
		uintptr(timeout),
	)
	if err := CheckError(uint32(ret)); err != nil {
		if str, err2 := j.PassThruGetLastError(); err2 == nil {
			return 0, nil, fmt.Errorf("%s: %w", str, err)
		} else {
			return 0, nil, err
		}
	}
	return int(*numMsgs), rMsgs, nil
}

func (j *PassThru) PassThruWriteMsg(channelID uint32, pMsg *PassThruMsg, timeout uint32) error {
	pNumMsgs := uint32(1)
	// long PassThruWriteMsgs(unsigned long ChannelID, PassThruMsg *pMsg, unsigned long *pNumMsgs, unsigned long Timeout);
	ret, _, _ := j.passThruWriteMsgs.Call(
		uintptr(channelID),
		uintptr(unsafe.Pointer(pMsg)),
		uintptr(unsafe.Pointer(&pNumMsgs)),
		uintptr(timeout),
	)
	return CheckError(uint32(ret))
}

func (j *PassThru) PassThruWriteMsgs(channelID uint32, pMsg *PassThruMsg, pNumMsgs *uint32, timeout uint32) error {
	// long PassThruWriteMsgs(unsigned long ChannelID, PassThruMsg *pMsg, unsigned long *pNumMsgs, unsigned long Timeout);
	ret, _, _ := j.passThruWriteMsgs.Call(
		uintptr(channelID),
		uintptr(unsafe.Pointer(pMsg)),
		uintptr(unsafe.Pointer(pNumMsgs)),
		uintptr(timeout),
	)
	return CheckError(uint32(ret))
}

func (j *PassThru) PassThruStartMsgFilter(channelID, filterType uint32, pMaskMsg, pPatternMsg, pFlowControlMsg *PassThruMsg, pMsgID *uint32) error {
	// long PassThruStartMsgFilter(unsigned long ChannelID, unsigned long FilterType, PassThruMsg *pMaskMsg, PassThruMsg *pPatternMsg, PassThruMsg *pFlowControlMsg, unsigned long *pMsgID);
	ret, _, _ := j.passThruStartMsgFilter.Call(
		uintptr(channelID),
		uintptr(filterType),
		uintptr(unsafe.Pointer(pMaskMsg)),
		uintptr(unsafe.Pointer(pPatternMsg)),
		uintptr(unsafe.Pointer(pFlowControlMsg)),
		uintptr(unsafe.Pointer(pMsgID)),
	)
	return CheckError(uint32(ret))
}

func (j *PassThru) PassThruReadVersion(deviceID uint32) (string, string, string, error) {
	var pFirmwareVersion [80]byte
	var pDllVersion [80]byte
	var pApiVersion [80]byte

	// long PassThruReadVersion(unsigned long DeviceID, char *pFirmwareVersion, char *pDllVersion, char *pApiVersion);
	ret, _, _ := j.passThruReadVersionProc.Call(
		uintptr(deviceID),
		uintptr(unsafe.Pointer(&pFirmwareVersion)),
		uintptr(unsafe.Pointer(&pDllVersion)),
		uintptr(unsafe.Pointer(&pApiVersion)),
	)

	if err := CheckError(uint32(ret)); err != nil {
		return "", "", "", err
	}

	return string(bytes.Trim(pFirmwareVersion[:], "\x00")), string(bytes.Trim(pDllVersion[:], "\x00")), string(bytes.Trim(pApiVersion[:], "\x00")), nil
}

// long PassThruIoctl(unsigned long HandleID, unsigned long IoctlID, void *pInput, void *pOutput);
func (j *PassThru) PassThruIoctl(handleID, ioctlID uint32, opts ...interface{}) error {
	switch ioctlID {
	case SET_CONFIG, GET_CONFIG:
		ret, _, _ := j.passThruIoctl.Call(
			uintptr(handleID),
			uintptr(ioctlID),
			uintptr(unsafe.Pointer(opts[0].(*SCONFIG_LIST))),
			uintptr(0),
		)
		return CheckError(uint32(ret))
	case CLEAR_MSG_FILTERS, CLEAR_RX_BUFFER, CLEAR_TX_BUFFER:
		ret, _, _ := j.passThruIoctl.Call(
			uintptr(handleID),
			uintptr(ioctlID),
			uintptr(0),
			uintptr(0),
		)
		return CheckError(uint32(ret))
	case FAST_INIT:
		if len(opts) != 2 {
			return ErrInvalidParameter
		}
		ret, _, _ := j.passThruIoctl.Call(
			uintptr(handleID),
			uintptr(ioctlID),
			uintptr(unsafe.Pointer(opts[0].(*PassThruMsg))),
			uintptr(unsafe.Pointer(opts[1].(*PassThruMsg))),
		)
		return CheckError(uint32(ret))
	}
	return ErrNotSupported
}

// long PassThruGetLastError(char *pErrorDescription);
func (j *PassThru) PassThruGetLastError() (string, error) {
	var pErrorDescription [80]byte
	ret, _, _ := j.passThruGetLastError.Call(
		uintptr(unsafe.Pointer(&pErrorDescription)),
	)
	return string(bytes.Trim(pErrorDescription[:], "\x00")), CheckError(uint32(ret))
}

func FindDLLs() (dlls []J2534DLL) {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\PassThruSupport.04.04`, registry.QUERY_VALUE|registry.WOW64_32KEY)
	if err != nil {
		//log.Println(err)
		return
	}
	ki, err := k.Stat()
	if err != nil {
		//log.Println(err)
		return
	}

	if err := k.Close(); err != nil {
		//log.Println(err)
		return
	}

	k2, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\PassThruSupport.04.04`, registry.ENUMERATE_SUB_KEYS|registry.WOW64_32KEY)
	if err != nil {
		//log.Println(err)
		return
	}

	adapters, err := k2.ReadSubKeyNames(int(ki.SubKeyCount))
	if err != nil {
		//log.Println(err)
		return
	}

	var capabilities Capabilities
	for _, adapter := range adapters {
		k3, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\PassThruSupport.04.04\`+adapter, registry.QUERY_VALUE|registry.WOW64_32KEY)
		if err != nil {
			continue
		}
		name, _, err := k3.GetStringValue("Name")
		if err != nil {
			continue
		}
		functionLibrary, _, err := k3.GetStringValue("FunctionLibrary")
		if err != nil {
			continue
		}
		if val, _, err := k3.GetIntegerValue("CAN"); err == nil {
			capabilities.CAN = val == 1
		}
		if val, _, err := k3.GetIntegerValue("CAN_PS"); err == nil {
			capabilities.CANPS = val == 1
		}
		if val, _, err := k3.GetIntegerValue("ISO9141"); err == nil {
			capabilities.ISO9141 = val == 1
		}
		if val, _, err := k3.GetIntegerValue("ISO15765"); err == nil {
			capabilities.ISO15765 = val == 1
		}
		if val, _, err := k3.GetIntegerValue("ISO14230"); err == nil {
			capabilities.ISO14230 = val == 1
		}
		if val, _, err := k3.GetIntegerValue("SW_CAN_PS"); err == nil {
			capabilities.SWCANPS = val == 1 || strings.ToLower(name) == "tech2"
		} else {
			if strings.ToLower(name) == "tech2" {
				capabilities.SWCANPS = true
			}
		}
		dlls = append(dlls, J2534DLL{Name: name, FunctionLibrary: functionLibrary, Capabilities: capabilities})
	}
	return
}
