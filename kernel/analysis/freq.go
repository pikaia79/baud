package analysis

type TermLocation struct {
	Field          string
	Start          int
	End            int
	Position       int
}

type TokenFreq struct {
	Term          []byte
	Locations     []*TermLocation
	frequency     int
}

func (tf *TokenFreq) Frequency() int {
	return tf.frequency
}

type TokenFrequencies map[string]*TokenFreq

func (tfs TokenFrequencies) MergeAll(field string, tokenFreq TokenFrequencies) {
	for term, tf := range tokenFreq {
		for _, l := range tf.Locations {
			l.Field = field
		}
		if existingTf, find := tfs[term]; find {
			existingTf.Locations = append(existingTf.Locations, tf.Locations...)
			existingTf.frequency = existingTf.frequency + tf.frequency
		} else {
			tfs[term] = &TokenFreq{
				Term:      tf.Term,
				frequency: tf.frequency,
				Locations: make([]*TermLocation, len(tf.Locations)),
			}
			copy(tfs[term].Locations, tf.Locations)
		}
	}
}

func TokenFrequency(tokens []*Token, includeTermVectors bool) TokenFrequencies {
	rv := make(map[string]*TokenFreq, len(tokens))

	if includeTermVectors {
		tls := make([]TermLocation, len(tokens))
		tlNext := 0

		for _, token := range tokens {
			tls[tlNext] = TermLocation {
				Start:          token.Start,
				End:            token.End,
				Position:       token.Position,
			}

			curr, ok := rv[string(token.Term)]
			if ok {
				curr.Locations = append(curr.Locations, &tls[tlNext])
				curr.frequency++
			} else {
				rv[string(token.Term)] = &TokenFreq{
					Term:      token.Term,
					Locations: []*TermLocation{&tls[tlNext]},
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
