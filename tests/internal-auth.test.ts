import assert from "node:assert/strict";
import test from "node:test";
import { isInternalKeyAuthorized } from "../src/index.ts";

test("internal auth accepts the primary key", () => {
  assert.equal(isInternalKeyAuthorized("primary", { BWM_INTERNAL_KEY: "primary" }), true);
});

test("internal auth accepts the optional next key", () => {
  assert.equal(isInternalKeyAuthorized("next", {
    BWM_INTERNAL_KEY: "primary",
    BWM_INTERNAL_KEY_NEXT: "next",
  }), true);
});

test("internal auth rejects the wrong key", () => {
  assert.equal(isInternalKeyAuthorized("wrong", {
    BWM_INTERNAL_KEY: "primary",
    BWM_INTERNAL_KEY_NEXT: "next",
  }), false);
});

test("internal auth rejects next when the primary is empty", () => {
  assert.equal(isInternalKeyAuthorized("next", {
    BWM_INTERNAL_KEY: "",
    BWM_INTERNAL_KEY_NEXT: "next",
  }), false);
});
