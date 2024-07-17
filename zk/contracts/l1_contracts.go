package contracts

import (
	"github.com/gateway-fm/cdk-erigon-lib/common"
)

var (
	SequencedBatchTopicPreEtrog    = common.HexToHash("0x303446e6a8cb73c83dff421c0b1d5e5ce0719dab1bff13660fc254e58cc17fce")
	SequencedBatchTopicEtrog       = common.HexToHash("0x3e54d0825ed78523037d00a81759237eb436ce774bd546993ee67a1b67b6e766")
	VerificationValidiumTopicEtrog = common.HexToHash("0x9c72852172521097ba7e1482e6b44b351323df0155f97f4ea18fcec28e1f5966")
	VerificationTopicPreEtrog      = common.HexToHash("0xcb339b570a7f0b25afa7333371ff11192092a0aeace12b671f4c212f2815c6fe")
	VerificationTopicEtrog         = common.HexToHash("0xd1ec3a1216f08b6eff72e169ceb548b782db18a6614852618d86bb19f3f9b0d3")
	UpdateL1InfoTreeTopic          = common.HexToHash("0xda61aa7823fcd807e37b95aabcbe17f03a6f3efd514176444dae191d27fd66b3")
	InitialSequenceBatchesTopic    = common.HexToHash("0x060116213bcbf54ca19fd649dc84b59ab2bbd200ab199770e4d923e222a28e7f")
	SequenceBatchesTopic           = common.HexToHash("0x3e54d0825ed78523037d00a81759237eb436ce774bd546993ee67a1b67b6e766")
)
