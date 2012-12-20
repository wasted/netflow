package io.netflow.flows.cisco

import io.wasted.util._

import io.netty.buffer._
import io.netty.util.CharsetUtil

import java.net.{ InetAddress, InetSocketAddress }

object FieldDefinition {
  val InBYTES_32 = 1
  val InPKTS_32 = 2
  val FLOWS = 3
  val PROT = 4
  val SRC_TOS = 5
  val TCP_FLAGS = 6
  val L4_SRC_PORT = 7
  val IPV4_SRC_ADDR = 8
  val SRC_MASK = 9
  val INPUT_SNMP = 10
  val L4_DST_PORT = 11
  val IPV4_DST_ADDR = 12
  val DST_MASK = 13
  val OUTPUT_SNMP = 14
  val IPV4_NEXT_HOP = 15
  val SRC_AS = 16
  val DST_AS = 17
  val BGP_IPV4_NEXT_HOP = 18
  val IPM_DPKTS = 19
  val IPM_DOCTETS = 20
  val LAST_SWITCHED = 21
  val FIRST_SWITCHED = 22
  val BYTES_64 = 23
  val PKTS_64 = 24
  val MIN_PKT_LEN = 25
  val MAX_PKT_LEN = 26
  val IPV6_SRC_ADDR = 27
  val IPV6_DST_ADDR = 28
  val IPV6_SRC_MASK = 29
  val IPV6_DST_MASK = 30
  val IPV6_FLOW_LABEL = 31
  val ICMP_TYPE = 32
  val MUL_IGMP_TYPE = 33
  val SAMPLING_INTERVAL = 34
  val SAMPLING_ALGORITHM = 35
  val FLOW_ACTIVE_TIMEOUT = 36
  val FLOW_INACTIVE_TIMEOUT = 37
  val ENGINE_TYPE = 38
  val ENGINE_ID = 39
  val TOTAL_BYTES_EXPORTED = 40
  val TOTAL_EXPORT_PKTS_SENT = 41
  val TOTAL_FLOWS_EXPORTED = 42
  val IPV4_SRC_PREFIX = 44
  val IPV4_DST_PREFIX = 45
  val MPLS_TOP_LABEL_TYPE = 46
  val MPLS_TOP_LABEL_IP_ADDR = 47
  val FLOW_SAMPLER_ID = 48
  val FLOW_SAMPLER_MODE = 49
  val FLOW_SAMPLER_RANDOM_INTERVAL = 50
  val MIN_TTL = 52
  val MAX_TTL = 53
  val IPV4_IDENT = 54
  val DST_TOS = 55
  val IN_SRC_MAC = 56
  val OUT_DST_MAC = 57
  val SRC_VLAN = 58
  val DST_VLAN = 59
  val IP_PROTOCOL_VERSION = 60
  val DIRECTION = 61
  val IPV6_NEXT_HOP = 62
  val BGP_IPV6_NEXT_HOP = 63
  val IPV6_OPTION_HEADERS = 64
  val MPLS_LABEL_1 = 70
  val MPLS_LABEL_2 = 71
  val MPLS_LABEL_3 = 72
  val MPLS_LABEL_4 = 73
  val MPLS_LABEL_5 = 74
  val MPLS_LABEL_6 = 75
  val MPLS_LABEL_7 = 76
  val MPLS_LABEL_8 = 77
  val MPLS_LABEL_9 = 78
  val MPLS_LABEL_10 = 79
  val IN_DST_MAC = 80
  val OUT_SRC_MAC = 81
  val IF_NAME = 82
  val IF_DESC = 83
  val SAMPLER_NAME = 84
  val IN_PERMANENT_BYTES = 85
  val IN_PERMANENT_PKTS = 86
}
