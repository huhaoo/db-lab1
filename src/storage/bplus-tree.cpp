#include "bplus-tree.hpp"

namespace wing {

InnerSlot InnerSlotParse(std::string_view slot) {
	InnerSlot rt;
	rt.next=*(pgid_t *)slot.data();
	rt.strict_upper_bound=slot.substr(sizeof(pgid_t));
	return rt;
}
size_t InnerSlotSerialize(char *s, InnerSlot slot) {
	*(pgid_t*)s=slot.next;
	memcpy(s+sizeof(pgid_t),slot.strict_upper_bound.data(),slot.strict_upper_bound.size());
	size_t size=sizeof(pgid_t)+slot.strict_upper_bound.size();
	s[size]='\0'; return size;
}

LeafSlot LeafSlotParse(std::string_view slot) {
	LeafSlot rt; pgoff_t len=*(pgoff_t*)slot.data();
	rt.key=slot.substr(sizeof(pgoff_t),len);
	rt.value=slot.substr(sizeof(pgoff_t)+len);
	return rt;
}
size_t LeafSlotSerialize(char *s, LeafSlot slot) {
	*(pgoff_t*)s=slot.key.size();
	memcpy(s+sizeof(pgoff_t),                slot.key.data(),  slot.key.size());
	memcpy(s+sizeof(pgoff_t)+slot.key.size(),slot.value.data(),slot.value.size());
	size_t size=sizeof(pgoff_t)+slot.key.size()+slot.value.size();
	s[size]='\0'; return size;
}

}
