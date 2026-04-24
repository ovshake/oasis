import { api } from "@/lib/api";
import { ScenarioForm } from "@/components/ScenarioForm";

interface Props {
  params: Promise<{ name: string }>;
}

export default async function EditScenarioPage({ params }: Props) {
  const { name } = await params;

  let initial;
  try {
    initial = await api.getScenario(name);
  } catch {
    // Scenario not found — fall back to blank form with the name pre-filled
    initial = { name };
  }

  return <ScenarioForm initial={initial} />;
}
