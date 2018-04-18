package analysis

type TokenInfo struct {
	Field          string
	ArrayPositions []uint64
	Start          int
	End            int
	Position       int
}

type TokenFreq struct {
	Term      []byte
	Infos     []*TokenInfo
	frequency int
}

func (tf *TokenFreq) Frequency() int {
	return tf.frequency
}

// TokenFrequencies maps document terms to their combined
// frequencies from all fields.
type TokenFrequencies map[string]*TokenFreq


func TokenFrequency(tokens []*Token, arrayPositions []uint64, includeTermVectors bool) TokenFrequencies {
	rv := make(map[string]*TokenFreq, len(tokens))

	if includeTermVectors {
		tls := make([]TokenInfo, len(tokens))
		tlNext := 0

		for _, token := range tokens {
			tls[tlNext] = TokenInfo{
				ArrayPositions: arrayPositions,
				Start:          token.Start,
				End:            token.End,
				Position:       token.Position,
			}

			curr, ok := rv[string(token.Term)]
			if ok {
				curr.Infos = append(curr.Infos, &tls[tlNext])
				curr.frequency++
			} else {
				rv[string(token.Term)] = &TokenFreq{
					Term:      token.Term,
					Infos: []*TokenInfo{&tls[tlNext]},
					frequency: 1,
				}
			}

			tlNext++
		}
	} else {
		for _, token := range tokens {
			curr, exists := rv[string(token.Term)]
			if exists {
				curr.frequency++
			} else {
				rv[string(token.Term)] = &TokenFreq{
					Term:      token.Term,
					frequency: 1,
				}
			}
		}
	}

	return rv
}
