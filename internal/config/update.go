package config

import (
	"reflect"
	"strings"

	json "github.com/bytedance/sonic"
)

// PreserveMissingSections restores, from src, every configuration field whose
// JSON key is absent from the submitted body. It is the merge step behind PATCH
// /api/config — and ONLY that verb: the handler decodes the body into a zero
// Config, so any section the caller omitted would otherwise become its zero
// value and the subsequent Save would erase it from disk (a partial update
// without "debrids" wiped every configured provider, api keys included).
//
// PUT /api/config deliberately does NOT call this: a full replacement means the
// omitted fields revert to their zero value, and softening that would make PUT
// indistinguishable from PATCH.
//
// The merge is RECURSIVE: it applies inside submitted sections too, not only at
// the top level. Submitting `{"repair":{"enabled":true}}` used to replace the
// WHOLE repair block, silently zeroing max_deletions_per_run (the
// destructive-action cap), stop_schedule, prune and regrab; now only "enabled"
// changes and the unmentioned knobs keep their stored values.
//
// Key presence is what separates "leave it alone" from "clear it": a key absent
// from body keeps the current value, while an explicitly submitted empty value
// (`"debrids": []`, `"download_folder": ""`, `"repair":{"prune":false}`) still
// overwrites. Bodies that carry every key (the web UI's full-config save) are
// unaffected because nothing is missing.
//
// Fields tagged `json:"-"` (e.g. Auth) are never copied here; the API handler
// manages those explicitly. Key matching prefers the exact JSON name and falls
// back to a case-insensitive match, mirroring the JSON decoder.
func (c *Config) PreserveMissingSections(src *Config, body []byte) error {
	return preserveMissingFields(reflect.ValueOf(c).Elem(), reflect.ValueOf(src).Elem(), body)
}

// PreserveMissingFields is the RepairConfig-level equivalent of
// PreserveMissingSections, and exists for the same reason: PATCH
// /api/repair/config decodes the submitted body into a zero RepairConfig, so
// without this merge every key the caller omitted would be silently reset —
// max_deletions_per_run to 0, stop_schedule to "" (stop schedule disabled),
// prune/regrab to false — which is not what a PARTIAL update promises.
//
// PUT /api/repair/config does not call this. There, clearing the omitted fields
// IS the contract, and it is safe because each knob's zero value is the
// conservative one (cap 0 ⇒ the default 100, prune/regrab false ⇒ delete
// nothing, repair nil ⇒ re-acquire).
//
// The Repair *bool tri-state is preserved exactly: an absent "repair" key keeps
// the current pointer (which may itself be nil, i.e. unset ⇒ defaults true),
// while an explicitly submitted true/false overwrites it.
func (r *RepairConfig) PreserveMissingFields(src RepairConfig, body []byte) error {
	return preserveMissingFields(reflect.ValueOf(r).Elem(), reflect.ValueOf(src), body)
}

// preserveMissingFields copies onto dst, from src, every field whose JSON key
// is absent from the posted body. dst must be a settable struct value and src a
// struct value of the same type.
func preserveMissingFields(dst, from reflect.Value, body []byte) error {
	var posted map[string]any
	if err := json.Unmarshal(body, &posted); err != nil {
		return err
	}
	mergeMissingFields(dst, from, posted)
	return nil
}

// mergeMissingFields walks dst/from in lockstep against the decoded body.
//
// Key presence — not the decoded value — separates "leave it alone" from "clear
// it", so an explicitly posted zero value still overwrites and a *bool that was
// never mentioned keeps its current pointer (nil included). Fields tagged
// `json:"-"` are never copied.
//
// Value kinds are treated as follows:
//
//   - struct (and pointer-to-struct): recursed into, so a partially posted
//     section keeps the fields it did not mention. This is what protects
//     repair.max_deletions_per_run from a `{"repair":{"enabled":true}}` PATCH.
//   - slice/array (debrids, arrs, usenet.providers, repair.arrs, …): NOT
//     element-merged. A posted array is the caller's complete list and REPLACES
//     the stored one wholesale (index is not identity — element-merging would
//     graft one provider's api key onto another). An absent array is preserved.
//   - map (custom_folders, …): NOT key-merged, for the same reason. A posted
//     object replaces wholesale; an absent one is preserved. Note this is the
//     one place a JSON object does not recurse — the distinction is the Go
//     type, not the JSON shape.
//   - everything else (scalars, *bool): the decoded value stands.
//
// A posted `null` for a struct field is "explicitly present" and therefore
// clears that section, exactly as before this became recursive.
func mergeMissingFields(dst, from reflect.Value, posted map[string]any) {
	folded := make(map[string]any, len(posted))
	for key, value := range posted {
		folded[strings.ToLower(key)] = value
	}

	for i, t := 0, dst.Type(); i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}

		value, ok := posted[name]
		if !ok {
			value, ok = folded[strings.ToLower(name)]
		}
		if !ok {
			// Never mentioned: keep what is currently configured.
			dst.Field(i).Set(from.Field(i))
			continue
		}

		// The key IS present, so the caller spoke about this field and the
		// decoded value stands — except for nested structs, which are merged
		// field by field so the keys the caller left out of the object are
		// preserved too.
		nested, isObject := value.(map[string]any)
		if !isObject {
			continue
		}
		if d, f, mergeable := structPair(dst.Field(i), from.Field(i)); mergeable {
			mergeMissingFields(d, f, nested)
		}
	}
}

// structPair resolves a dst/from field pair to the struct values the recursion
// needs, following one level of pointer indirection. It reports false for
// anything that is not a struct of the same type on both sides (maps included —
// see mergeMissingFields), and for a pointer that is nil on either side, where
// there is either nothing to merge into or nothing left to preserve.
func structPair(dst, from reflect.Value) (reflect.Value, reflect.Value, bool) {
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() || from.Kind() != reflect.Pointer || from.IsNil() {
			return reflect.Value{}, reflect.Value{}, false
		}
		dst, from = dst.Elem(), from.Elem()
	}
	if dst.Kind() != reflect.Struct || from.Kind() != reflect.Struct || dst.Type() != from.Type() {
		return reflect.Value{}, reflect.Value{}, false
	}
	return dst, from, true
}
