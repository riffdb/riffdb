declare global {
  interface Window {
    turnstile?: { reset: (widget?: string | HTMLElement) => void };
  }
}

const form = document.querySelector<HTMLFormElement>("[data-waitlist-form]");

if (form?.dataset.configured === "true") {
  const email = form.elements.namedItem("email") as HTMLInputElement;
  const website = form.elements.namedItem("website") as HTMLInputElement;
  const button = form.querySelector<HTMLButtonElement>("button[type='submit']");
  const status = form.querySelector<HTMLElement>("[data-form-status]");

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    status?.classList.remove("is-error", "is-success");

    if (!email.checkValidity()) {
      email.reportValidity();
      return;
    }

    const token =
      new FormData(form).get("cf-turnstile-response")?.toString() ?? "";
    if (!token) {
      if (status) {
        status.textContent =
          "Please complete the verification, then try again.";
        status.classList.add("is-error");
      }
      return;
    }

    if (button) {
      button.disabled = true;
      button.textContent = "Joining…";
    }

    try {
      const response = await fetch("/api/waitlist", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          email: email.value,
          turnstileToken: token,
          website: website.value,
        }),
      });
      const result = (await response.json()) as {
        ok?: boolean;
        message?: string;
      };

      if (status) {
        status.textContent =
          result.message ?? "Something went wrong. Please try again.";
        status.classList.add(
          response.ok && result.ok ? "is-success" : "is-error",
        );
      }
      if (response.ok && result.ok) {
        form.reset();
      }
    } catch {
      if (status) {
        status.textContent =
          "We couldn’t reach the signup service. Please try again.";
        status.classList.add("is-error");
      }
    } finally {
      if (button) {
        button.disabled = false;
        button.textContent = "Join early access";
      }
      window.turnstile?.reset();
    }
  });
}

export {};
