/// A single loaded material slot, as the overlay should display it.
///
/// `label` is the slot identifier shown on the spool tag — `"1".."N"` for
/// AMS-style slots, `"ext"` for an external feeder, `"T0".."T3"` for
/// Klipper-style multi-tool printers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Material {
    pub(crate) label: String,
    pub(crate) kind: String,
    pub(crate) color: String,
    pub(crate) active: bool,
}
