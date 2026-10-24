package rtc

import "strings"

// acceptBundleOnlyData makes an offer whose data channel section is "bundle-only" (port 0 with
// a=bundle-only, RFC 8843) acceptable to pion.
//
// Recent Firefox and Safari put the data channel behind the first media section of a BUNDLE group
// this way, and pion starts SCTP only for a data channel section with a non-zero port: it takes
// port 0 for a rejected section, as it never does for a bundle-only audio or video section. The
// connection comes up, but no data channel ever opens, and with it nothing is forwarded to the
// participant (the SFU waits for the data channel before it sends or negotiates anything).
//
// Such a section is rewritten to the form Chrome sends: port 9, without a=bundle-only. The
// answerer bundles it all the same, so the meaning is unchanged. Anything else is left alone.
func acceptBundleOnlyData(sdp string) string {
	if !strings.Contains(sdp, "m=application 0 ") || !strings.Contains(sdp, "a=bundle-only") {
		return sdp
	}
	lines := strings.SplitAfter(sdp, "\n")
	var out strings.Builder
	out.Grow(len(sdp))
	for i := 0; i < len(lines); {
		if !strings.HasPrefix(lines[i], "m=") {
			out.WriteString(lines[i])
			i++
			continue
		}
		end := i + 1
		for end < len(lines) && !strings.HasPrefix(lines[end], "m=") {
			end++
		}
		section := lines[i:end]
		if strings.HasPrefix(section[0], "m=application 0 ") && hasLine(section, "a=bundle-only") {
			out.WriteString("m=application 9 " + strings.TrimPrefix(section[0], "m=application 0 "))
			for _, line := range section[1:] {
				if strings.TrimRight(line, "\r\n") != "a=bundle-only" {
					out.WriteString(line)
				}
			}
		} else {
			for _, line := range section {
				out.WriteString(line)
			}
		}
		i = end
	}
	return out.String()
}

func hasLine(lines []string, want string) bool {
	for _, line := range lines {
		if strings.TrimRight(line, "\r\n") == want {
			return true
		}
	}
	return false
}
